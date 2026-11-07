# ModelGate

A Kubernetes operator that rolls out new model versions behind a **model-quality gate**. It sends a new version a small slice of live traffic, compares its predictions with the current version on the same traffic, and promotes or rolls back on its own. It checks prediction quality as well as latency and errors.

A model can return HTTP 200 in 40 ms and still be wrong. ModelGate catches flipped decisions, class-mix shifts, score-distribution changes, NaN outputs, and slow or failing servers within minutes, using live traffic and no ground-truth labels.

## Status: v0.1 foundation

| Area | State |
| --- | --- |
| `ModelRollout` CRD (v1alpha1), CEL validation (first step at most 50%) | done |
| Controller: shadow → weighted steps → promote/rollback, crash-safe status, kill switch, leader election | done; unit, envtest, and live on kind |
| Gateway API traffic: HTTPRoute weights + `RequestMirror` for shadow | done |
| Capture sidecar (Go reverse proxy, fail-open, sampled, JSONPath extraction) | done |
| Analyzer: t-digest sketches, 9 checks, alpha-spending sequential bounds | done |
| Analyzer as Argo Rollouts web metric / Flagger webhook | done |
| Transport: NATS JetStream (replay on analyzer restart) or direct HTTP | done |
| `kubectl modelgate` plugin (status/pause/resume/abort/promote --force) with identity | done |
| Helm chart, Dockerfiles, CI | done; installed and exercised on kind with Envoy Gateway |
| Demo model with the PRD's six bad variants | done |
| Namespace defaults (`ModelGatePolicy`) enforced at runtime + validating webhook | done; envtest and live on kind |
| PostgreSQL audit log (verdicts, disagreement samples, every decision and actor) | done; unit, real Postgres, and live on kind |
| Grafana dashboard (Helm ConfigMap for the Grafana sidecar) | done |
| Delayed-label accuracy (`labelAccuracy` check, `/v1/outcomes` API) | done; unit, benchmark, live on kind |
| Opt-in, time-limited payload capture with redaction in the sidecar | done; unit and controller tests |
| Replay of recorded requests for low-traffic models | done; unit, envtest, and kind e2e with no live traffic |
| KServe v2 / Triton: REST and gRPC tensors, GRPCRoute | done; unit, real gRPC client and server, local smoke, kind e2e |
| Bad-model benchmark (50 runs per variant), sidecar load test | done |
| kind e2e in CI (NATS, Postgres, Envoy Gateway), Chaos Mesh run | done; see [hack/e2e/run.sh](hack/e2e/run.sh) |
| Release: multi-arch images, SBOMs, cosign signatures, vulnerability scans | done; [release workflow](.github/workflows/release.yaml), not yet run on a tag |

## How it works

```
             ┌──────────── HTTPRoute (weights + RequestMirror) ────────────┐
client ──► Gateway                                                         │
             ├──► <name>-stable Service ──► [capture sidecar → model vN]   │
             └──► <name>-canary Service ──► [capture sidecar → model vN+1] │
                                   │ prediction events (no raw payloads)
                                   ▼
                       NATS JetStream / HTTP ──► analyzer (sketches, checks)
                                                     ▲ verdict
                                  controller ────────┘  └─► HTTPRoute, Deployments, status, events
```

1. Changing `spec.template` creates revision `<name>-<hash>` next to the stable one.
2. **Shadow**: the gateway mirrors a share of requests to the canary. Users only see stable responses, and the analyzer pairs both responses by `X-Request-Id`. The gateway sets that ID and copies it onto the mirrored request; Envoy Gateway replaces client-supplied IDs with its own.
3. **Steps**: the canary serves 5% → 25% → 50% of live traffic. Each step compares canary and stable over the same time window.
4. When every stage passes, the canary becomes stable and the old revision is deleted. A failure moves all traffic back to stable, deletes the canary, and records why.

### Decision rules

- No statistical check is judged until `minSamples` is reached for both versions. Missing data **holds** a rollout and never promotes it.
- **Hard checks** (`validity`, `errorRate`) roll back immediately, even before `minSamples`, as soon as the evidence is conclusive.
- A check fails only when its **confidence bound** crosses the threshold, not just the point estimate.
- Every bound spends a false-alarm budget `alpha` across looks: look *k* in a stage gets `alpha/(k(k+1))`. That sums to `alpha` however often the controller polls. This answers the PRD's open question: the union bound gives a one-line false-alarm proof, and the Chernoff (binomial KL) bounds stay tight for rates near 0 or 1.
- A step passes when its pause has elapsed and the latest verdict passes. The rollout fails after `failureLimit` consecutive failed verdicts.
- If the analyzer is unreachable, the kill switch is on, or `spec.paused` is set, the rollout holds at its current weight.
- Looks are spaced by `analysis.interval`, so extra reconciles from watch events never count as additional failures.
- Each stage's window starts at the first sketch bucket after the stage began, so no traffic from the previous stage leaks in.
- `p99Ratio` is skipped in shadow unless its `phase` is set to `shadow`. Mirrored traffic is fire-and-forget (open-loop) while user traffic is closed-loop, so near capacity the canary queues more and looks slower even when the code is identical. On kind this produced a 2x p99 gap between identical models.
- Capture sampling (`capture.sampleRate`) hashes the request ID, so both sidecars record the same requests and shadow pairs survive sampling.

### Checks

| Type | Output | Fails when |
| --- | --- | --- |
| `agreementRate` (shadow) | classification | upper bound of paired agreement < `min` |
| `meanAbsDiff` (shadow) | regression, score | lower bound of mean \|canary − stable\| > `max` |
| `psi` | all (classes, or score deciles) | PSI > `max` **and** chi-square rejects equality |
| `chiSquare` | all | chi-square rejects **and** Cramér's V > `max` |
| `ks` | score, regression | KS − both DKW radii > `max` |
| `wasserstein` | score, regression | W₁ > `max` **and** KS rejects equality |
| `validity` (hard) | all | any invalid output when `max: 0`, else lower bound > `max` |
| `errorRate` (hard) | all | lower bound of canary 5xx rate > `max` |
| `p99Ratio` | all | canary p99 (lower band) / stable p99 (upper band) > `max` |
| `labelAccuracy` | classification | lower bound of (stable accuracy − canary accuracy) on delayed ground-truth labels > `max` (default 0.02) |

### Delayed labels

When true outcomes arrive later (a chargeback, a confirmed diagnosis), post them to the analyzer and add a `labelAccuracy` check:

```sh
curl -X POST http://modelgate-analyzer.modelgate-system.svc/v1/outcomes -d \
  '[{"rollout": "demo/demo-model", "requestId": "4f1c...", "label": "benign"}]'
```

```yaml
- { name: accuracy, type: labelAccuracy, max: 0.02, minSamples: 200 }
```

- Labels are joined to the captured predictions by request ID, so the client's `X-Request-Id` must be the ID it later reports the outcome for. Predictions wait up to an hour for their label. Gateways often replace that header: Envoy Gateway does unless a `ClientTrafficPolicy` sets `headers.requestID: Preserve`, as [config/samples/demo.yaml](config/samples/demo.yaml) does.
- Each label counts in the time bucket its prediction was served in. The check pools labels over the whole rollout, because the labels for one stage usually arrive after that stage has ended.
- `minSamples` on the check counts labeled predictions per version, 200 by default. Until there are enough, the check is inconclusive and the rollout holds at its current weight rather than promoting. A namespace policy's `minSamples` floor does not apply to label checks.

## Quickstart

### One command on kind

```sh
pip install -r demo/model/requirements.txt
hack/quickstart.sh       # cluster, images, ModelGate, audit DB, Prometheus, Grafana, demo traffic
hack/demo.sh bad swapped # ship a broken model and watch it roll back
hack/demo.sh good        # ship a retrained model and watch it promote
hack/demo.sh audit       # the audit trail
hack/quickstart.sh down  # stop the background traffic; "delete" removes the cluster
```

The script builds and installs everything from source and starts 50 req/s of demo traffic, plus ground-truth labels 20 s after each request. It then prints the Grafana URL (`http://localhost:3000/d/modelgate`) and the next commands. The demo rollout uses short stages (shadow 60 s, then 20% and 50% for 45 s each), so a good model promotes in about three minutes. `kubectl modelgate status demo-model -n demo -w` follows a rollout live.

### Local, no cluster (about 1 minute)

Runs the real analyzer and sidecar binaries in front of two demo model servers, with mirroring simulated:

```sh
pip install -r demo/model/requirements.txt
make smoke          # or: go build -o bin/ ./cmd/... && python hack/local_smoke.py --variant swapped
```

Variants: `good identical swapped scaling stale accuracy-drop nan slow`.

### kind cluster

```sh
make kind-up        # kind + Envoy Gateway + GatewayClass "eg"
make kind-load      # build and load images
make deploy         # helm install into modelgate-system
make demo           # Gateway, HTTPRoute, ModelRollout in namespace demo
# start a rollout with a broken model:
kubectl -n demo patch modelrollout demo-model --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/env/0/value","value":"swapped"}]'
kubectl modelgate status demo-model -n demo
```

> Helm does not upgrade CRDs in `crds/`. After changing the CRD run `kubectl apply --server-side -f config/crd/`; it is too large for client-side apply. Shadow sampling uses the Gateway API `RequestMirror.percent` field, which is in the experimental channel. With standard-channel CRDs, mirroring falls back to 100%.

## Namespace policy

Platform teams set a floor once per namespace with a `ModelGatePolicy` named `default` ([config/samples/policy.yaml](config/samples/policy.yaml)). Teams may tighten it, never loosen it.

- **The controller is the guard.** Every reconcile merges the policy into the rollout in memory, taking the stricter threshold for each check type and phase. Required checks the rollout doesn't declare are added as `policy-<name>`. The policy also sets a floor on `minSamples` and caps `failureLimit`, `alpha` and the first step's weight. The stored spec is never rewritten. The `PolicyApplied` condition says what was raised. If the policy can't be read, the rollout doesn't proceed.
- **The webhook gives feedback.** It rejects a create or spec update that is looser than the policy and names every offending field. Metadata-only updates always pass, so `kubectl modelgate abort` keeps working on rollouts that predate a stricter policy. It fails open (`failurePolicy: Ignore`), because runtime enforcement covers safety. The Helm chart generates its TLS certificate at install time and reuses it on upgrade; cert-manager is not needed.

## Manual control

```sh
kubectl modelgate pause   demo-model -n demo
kubectl modelgate resume  demo-model -n demo
kubectl modelgate abort   demo-model -n demo
kubectl modelgate promote demo-model -n demo --force
kubectl modelgate payloads demo-model -n demo   # see Payload capture
```

The plugin looks up the caller's identity with a `SelfSubjectReview`, and the controller writes it into the rollout's events. The cluster-wide kill switch is `freeze: "true"` in ConfigMap `modelgate-system/modelgate-config`.

## KServe v2, Triton and gRPC

Model servers that speak the Open Inference Protocol (KServe v2), such as Triton, KServe, MLServer and TorchServe, need no JSONPath. Name the output tensors instead:

```yaml
spec:
  routeRef: { name: fraud, kind: GRPCRoute }   # HTTPRoute (default) for REST
  output:
    type: classification
    protocol: kserveV2gRPC                      # or kserveV2 for REST, json (default) for anything else
    labelTensor: { name: probabilities, reduce: argmax }  # label is the class index
    scoreTensor: { name: probabilities, reduce: max }     # score is the top probability
```

- **Reading tensors.** Only the first row (the first entry along the batch dimension) is read. `reduce: first` (default) takes its first element, `argmax` the index of its largest element, and `max` the largest element. A label tensor can also be `BYTES`, like Triton's classification extension. Over gRPC, both typed `contents` and `raw_output_contents` are decoded, for every datatype including FP16 and BF16. NaN values count as invalid outputs.
- **Which calls count.** With `kserveV2`, only POSTs to `.../infer` are recorded, and with `kserveV2gRPC`, only unary `ModelInfer` calls. Health, metadata and other calls pass through untouched. A gRPC error status is recorded as its HTTP equivalent, so `INVALID_ARGUMENT` is a client error and `INTERNAL` or `UNAVAILABLE` counts toward `errorRate`.
- **gRPC traffic.** The sidecar accepts HTTP/2 without TLS (h2c) and speaks it to the model server. The Services get `appProtocol: kubernetes.io/h2c`, and the controller steers a `GRPCRoute` the same way it steers an HTTPRoute: backend weights for steps, `RequestMirror` for shadow. [config/samples/demo-grpc.yaml](config/samples/demo-grpc.yaml) runs the demo model as a Triton-style gRPC server.
- **Limits.** Streaming gRPC calls pass through but are not analyzed. Payload capture keeps only JSON bodies, so it records no gRPC bodies. Replay sends JSON over HTTP, so it works with `kserveV2` REST but not with `kserveV2gRPC`; the CRD rejects that combination.

## Payload capture for debugging

By default the sidecar sends only the extracted prediction, latency and status, never request or response bodies. When a disagreement needs a closer look, turn on payload capture for a limited time:

```yaml
spec:
  capture:
    payloads:
      until: "2026-11-07T18:00:00Z"   # at most 24 hours ahead
      sampleRate: 0.01                # share of requests, default 0.01
      maxBytes: 8192                  # largest body kept, default 8192
      redact: ["$.instances[*].ssn", "$.patient.*", "$['card number']"]
```

```sh
kubectl modelgate payloads demo-model -n demo                    # the 10 newest
kubectl modelgate payloads demo-model -n demo --request-id 4f1c  # stable and canary for one request
```

- **Redaction happens in the sidecar**, before anything leaves the pod: each path's values become `"[REDACTED]"`. Paths support `.key`, `['key']`, `[n]`, `[*]` and `.*`. Only JSON bodies are captured, because only a parsed body can be redacted reliably. Bodies that are not JSON, are truncated, or are larger than `maxBytes` are left out and the record says why. Headers are never captured.
- **It ends by itself.** The sidecar stops at `until`, so capture ends on time even if the controller is down. The webhook rejects an `until` more than 24 hours ahead; if the webhook is bypassed, the controller keeps capture off and says so in the `PayloadCapture` condition. Expiry does not restart pods. Turning capture on or off, or changing its settings, rolls the model pods, like any other capture setting.
- **Stable and canary keep the same requests.** Payload sampling uses the request ID, like event sampling, so you can compare both responses to one request. Each sidecar keeps at most 20 payloads per second.
- **Where it goes:** payloads travel with the events to the analyzer, which keeps the last 500 per rollout for an hour in memory and serves them at `GET /v1/payloads?rollout=ns/name[&requestId=...]`. With NATS, events, including payloads, are also stored in the JetStream stream for an hour. The plugin reads them through the API server's service proxy, so it needs `services/proxy` on `modelgate-analyzer` in `modelgate-system`. The analyzer API itself has no authentication; restrict it with a NetworkPolicy if other workloads share the cluster.
- **Namespace policy:** `spec.payloadCapture.disabled: true` in the `ModelGatePolicy` forbids capture in the namespace, and `spec.payloadCapture.redact` adds paths every rollout must redact.

## Replay for low-traffic models

A model that serves a few requests a minute may never reach `minSamples` on live traffic, so its rollouts hold forever. Replay sends recorded requests to both versions while a canary is under analysis:

```yaml
spec:
  strategy:
    shadow: { duration: 10m }
    replay:
      configMapRef: { name: fraud-replay, key: requests.jsonl }  # key defaults to requests.jsonl
      path: /predict          # where requests are POSTed, default /
      ratePerSecond: 20       # recorded requests per second, default 5, at most 200
```

The ConfigMap holds one JSON request body per line. To record real traffic, turn on [payload capture](#payload-capture-for-debugging) for a while and export the captured requests:

```sh
kubectl modelgate payloads fraud -n ml --limit 500 --requests > requests.jsonl
kubectl -n ml create configmap fraud-replay --from-file=requests.jsonl
```

- **Same request, both versions.** Each recorded request goes to the stable and canary Services with the same new request ID, so the analyzer pairs the two responses like mirrored traffic, and `agreementRate` and `meanAbsDiff` work with no live traffic at all. IDs are picked so the sidecars' `capture.sampleRate` keeps every replayed request.
- **Users never see replayed responses.** The controller sends them straight to the Services, not through the HTTPRoute. It cycles through the file for as long as the canary is under analysis, and stops when the rollout is paused, held by the kill switch or a missing analyzer, promoted or rolled back.
- **It adds load to stable.** Every replayed request also runs on the production model; size `ratePerSecond` for that. Exported requests are redacted, so a field your model needs must not be in the redact list, or the replayed predictions will differ from production.
- **Status:** the `Replay` condition says how many requests were loaded, or why replay is off (`ConfigMapNotFound`, `KeyNotFound`, `InvalidRequests`). `modelgate_replay_requests_total{target,outcome}` on the controller counts what was sent. The controller needs network access to the model Services; allow it if a NetworkPolicy restricts them.

## Audit log

Set `audit.existingSecret` in the Helm chart to a Secret holding a PostgreSQL DSN (key `dsn`), and the controller records every decision in table `modelgate_audit`, which it creates on first use. A demo database for kind is in [config/samples/postgres.yaml](config/samples/postgres.yaml).

- **What is recorded:** first deploys, rollout starts, every verdict with its checks and up to five disagreement samples (`verdict` jsonb), step advances, promotions, rollbacks, holds, pauses, resumes, aborts and forced promotions. Each row carries the actor: `system:modelgate-controller` for automatic decisions, the caller's Kubernetes identity for plugin actions.
- **Ordering:** entries are written before the status change they describe, so nothing takes effect without a record. Writes are idempotent, so a retried reconcile does not record a decision twice.
- **When the database is down:** step advances, promotions, new rollouts and forced promotions wait, and the rollout shows `Paused` with `audit log unavailable`. Rollbacks and aborts still happen. Their entries are dropped and an `AuditWriteFailed` event is emitted, because an audit outage must never keep a bad canary serving traffic.

```sql
SELECT recorded_at, action, actor, canary_revision, canary_weight, reason
FROM modelgate_audit WHERE namespace = 'demo' AND rollout = 'demo-model'
ORDER BY recorded_at DESC LIMIT 20;
```

Without a DSN, decisions are still recorded as Kubernetes events and in status.

## Dashboard

The chart installs ConfigMap `modelgate-dashboard` with label `grafana_dashboard: "1"`, which the Grafana sidecar in kube-prometheus-stack loads. It shows each rollout's phase, canary weight, verdicts, check values, promotions, rollbacks, reconcile errors, analyzer ingest, and sidecar capture outcomes. The JSON is in [charts/modelgate/dashboards/modelgate.json](charts/modelgate/dashboards/modelgate.json) for manual import; turn the ConfigMap off with `grafana.dashboard.enabled=false`.

## Releases

Pushing a tag `vX.Y.Z` runs [.github/workflows/release.yaml](.github/workflows/release.yaml). It publishes multi-arch (amd64, arm64) images for the controller, analyzer and sidecar to GHCR, scans them with Trivy, and attaches an SPDX SBOM to each one as a signed attestation. Images and the OCI Helm chart are signed with keyless cosign. Plugin binaries, CRDs and checksums go to the GitHub release, with the checksums signed. To verify an image:

```sh
cosign verify ghcr.io/<owner>/modelgate-controller:<version> \
  --certificate-identity-regexp 'https://github.com/<owner>/<repo>/.github/workflows/release.yaml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

Changes are listed in [CHANGELOG.md](CHANGELOG.md).

## Using only the analyzer (Argo Rollouts, Flagger)

- Argo Rollouts: see [config/samples/argo-analysistemplate.yaml](config/samples/argo-analysistemplate.yaml). `GET /v1/argo/verdict` returns `{"decision": "pass|fail|inconclusive", ...}`, and an inconclusive verdict pauses instead of promoting.
- Flagger: point a webhook at `POST /v1/flagger/check`. It takes the same keys in `metadata` (`stable`, `canary`, `window`, `minSamples`, `checks`) and returns 200 for pass, 412 for fail, and 425 for inconclusive.
- Compact check syntax: `name:type[:min=x|max=y][:phase=shadow|canary]`, comma-separated.

Model pods still need the capture sidecar, with `MODELGATE_REVISION` set to the pod-template hash.

## Layout

```
api/v1alpha1/          ModelRollout types (deepcopy + CRD generated by controller-gen)
internal/stats/        t-digest, KS/Wasserstein, Chernoff/DKW/quantile bounds, chi-square, PSI
internal/analysis/     time-bucketed per-revision sketches, shadow pairing, check engine
internal/analyzer/     HTTP API, Argo/Flagger adapters, NATS consumer, metrics
internal/capture/      sidecar proxy, JSONPath subset, batching publisher (HTTP/NATS)
internal/controller/   reconciler, Deployment/Service/HTTPRoute rendering, metrics
internal/policy/       ModelGatePolicy merge (teams may only tighten)
internal/webhook/      validating webhook for ModelRollouts
internal/audit/        audit log interface and PostgreSQL store
cmd/                   manager, analyzer, sidecar, kubectl-modelgate
charts/modelgate/      Helm chart, including the Grafana dashboard
demo/model/            scikit-learn demo model over JSON, KServe v2 REST and gRPC; bad variants, load generator
hack/                  kind setup, e2e and chaos suite, local smoke test, benchmark, sidecar load test
```

## Testing

```sh
make test       # vet + unit tests
make envtest    # controller against a real kube-apiserver + etcd
make smoke      # real analyzer + sidecars + demo models, no cluster
make bench      # bad-model suite, 50 runs per variant
make loadtest   # sidecar overhead at 1,000 req/s
make e2e        # kind: NATS, Postgres, Envoy Gateway, real rollouts under load
make e2e-chaos  # the same while Chaos Mesh kills the controller, analyzer and NATS
make e2e-replay # no live traffic: replayed requests alone must promote and roll back
make vulncheck  # govulncheck on the module
```

- **Unit:** sketch accuracy and bound coverage under repeated looks. 50 simulated identical-model rollouts stay under 2% false rollbacks, and each bad variant must fail on the expected check. Controller logic runs against a fake client.
- **envtest:** the webhook behind a real API server. A loose rollout is rejected with a reason, a compliant one is accepted, annotation-only updates pass after the policy tightens, and a second policy name is refused.
- **envtest:** the generated CRD schema, CEL rules, defaults, pod-template metadata, the status subresource, and the experimental mirror field are all enforced by a real API server. It also drives a full rollout: deploy, shadow, three steps, promote, then a bad canary rolled back.
- **Local smoke:** all 8 demo variants through the real binaries. `good` and `identical` pass; the 6 bad variants fail. `--protocol v2` and `--protocol grpc` run the same over KServe v2 REST and gRPC; CI runs both.
- **Audit:** controller tests check the recorded sequence for a full rollout, the actors for pause, resume and abort, and fail-safe behavior with the database down (holds, never promotes, still rolls back, a forced promote waits and is recorded once). `internal/audit` runs against a real PostgreSQL when `MODELGATE_TEST_POSTGRES_DSN` is set; CI runs it with a Postgres service container.
- **kind e2e (CI):** `hack/e2e/run.sh` installs everything with NATS JetStream and the audit log, sends load through Envoy Gateway, and runs rollouts of demo variants. Good variants must promote and bad ones must roll back; a bad variant ever becoming stable fails the run at once. It then checks the audit log against the outcomes. The CI chaos job runs the same suite while Chaos Mesh kills a controller replica every 40 s, the analyzer every 90 s, and NATS every 150 s. `CHAOS=kubectl` does the same kills without Chaos Mesh. The CI replay job sends no live traffic at all, so `identical` must promote and `swapped` must roll back on replayed requests alone; it reads the model through KServe v2 REST tensors. The CI grpc job runs the demo model as a KServe v2 gRPC server behind a GRPCRoute.
- **kind + Envoy Gateway:** tested by hand.
  - A good rollout promotes through every stage.
  - `swapped` and `nan` roll back in shadow with zero user exposure; `slow` rolls back at 5%.
  - Analyzer down for 80 s: the rollout holds, then resumes.
  - Leader controller killed mid-step: the standby takes over and finishes.
  - Namespace policy: the webhook rejects a looser spec, the runtime condition lists what was raised, and a 3% first-step cap reaches the HTTPRoute (97/3).
  - The kill switch freezes rollouts; the plugin's pause, resume, abort and promote guard all work, and an abort records `kubernetes-admin`.

### Benchmark results

`make bench`: real scikit-learn variant predictions, the real analysis engine, PRD rollout config (shadow 10m at 20%, then 5%/10m, 25%/15m, 50%/15m, `minSamples` 2000). Simulated time, 50 req/s, 50 runs each.

| Variant | Expected | Result | Median time to decision | Median user exposure | Caught by |
| --- | --- | --- | --- | --- | --- |
| identical | promote | 0/50 false rollbacks | 53.5 min (full rollout) | n/a | - |
| swapped labels | rollback | 50/50 | 4.0 min | 0% (shadow) | agreement |
| scaling bug | rollback | 50/50 | 4.0 min | 0% (shadow) | agreement |
| stale data | rollback | 50/50 | 4.0 min | 0% (shadow) | agreement |
| accuracy drop (0.959 to 0.901) | rollback | 50/50 | 4.0 min | 0% (shadow) | agreement |
| NaN outputs | rollback | 50/50 | 0.5 min | 0% (shadow) | validity |
| 2x latency | rollback | 50/50 | 24.0 min | 2.9% | latency at the 5% step |

The latency regression misses the 15-minute target at low traffic. It can only be judged on split traffic, and the 5% step needs 2000 canary samples: 17.5 min at 100 req/s, 14.0 min at 200 req/s. The accuracy-drop variant is caught because it disagrees with the good model on about 7% of requests. A regression that keeps decisions this close to the good model's would still need delayed labels. The identical variant has exactly equal predictions, so its false-alarm rate tests sampling noise in split traffic; the unit tests also cover noisy near-identical models.

#### With delayed labels only

The hardest case in the PRD: what if the label-free checks were blind to the accuracy drop? `go run ./hack/benchmark -variants identical,accuracy-drop -label-delay <d> -label-rate 0.5 -labels-only` removes every label-free quality check (only validity, latency and errors remain) and adds `labelAccuracy` (max drop 0.02). Half of the requests get their true diagnosis after the delay. The rollout config and 50 runs per variant are as above.

| Label delay | accuracy-drop caught | Median time to rollback | Median exposure | identical: false rollbacks | identical: median time to promote |
| --- | --- | --- | --- | --- | --- |
| 1 min | 50/50 | 6.5 min | 0% (shadow) | 0/50 | 53.5 min |
| 10 min | 50/50 | 15.0 min | 1.3% | 0/50 | 54.5 min |
| 30 min | 50/50 | 35.0 min | 0.56% | 0/50 | 74.5 min |

Ground truth alone catches the 5.8-point drop every time, roughly the label delay plus five minutes later. The cost is time: with 30-minute labels, a good model waits about 20 extra minutes in its last stage for enough labels before it is promoted.

### Sidecar overhead

`make loadtest`: open-loop at 1,000 req/s for 30 s, 5 ms model, capture on every request. The sidecar adds **0.13 ms at p99** (5.85 to 5.98 ms) and drops no events. Measured on one Windows dev machine, client and server on the same host, so treat it as indicative, not per-pod.

## Known limitations

- The canary runs the same replica count as stable.
- Only the current template can be rendered. If someone deletes the old stable Deployment mid-rollout, the rollout stalls instead of recreating it.
- The analyzer keeps sketches in memory. Run it with NATS so a restart can replay the last hour.
- A model whose decisions stay very close to the good model's, but are worse, gets past the label-free checks. The `accuracy-drop` variant is caught without labels only because it disagrees on about 7% of requests. For closer regressions, use `labelAccuracy`; how long it takes depends on how quickly labels arrive (see the benchmark).
- Delayed labels are supported for classification outputs only.

Licensed under Apache 2.0.
