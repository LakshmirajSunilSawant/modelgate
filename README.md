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
| Postgres audit log, Grafana dashboard | **not yet** |
| Delayed-label accuracy, replay mode, KServe/gRPC | **not yet** (post-v0.1) |
| Bad-model benchmark (50 runs per variant), sidecar load test | done |
| kind e2e in CI, Chaos Mesh tests, cosign/SBOM | **not yet** (resilience was checked by hand on kind) |

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

## Quickstart

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
```

The plugin looks up the caller's identity with a `SelfSubjectReview`, and the controller writes it into the rollout's events. The cluster-wide kill switch is `freeze: "true"` in ConfigMap `modelgate-system/modelgate-config`.

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
cmd/                   manager, analyzer, sidecar, kubectl-modelgate
charts/modelgate/      Helm chart
demo/model/            FastAPI + scikit-learn demo model, bad variants, load generator
hack/                  kind setup, local smoke test, benchmark, sidecar load test
```

## Testing

```sh
make test       # vet + unit tests
make envtest    # controller against a real kube-apiserver + etcd
make smoke      # real analyzer + sidecars + demo models, no cluster
make bench      # bad-model suite, 50 runs per variant
make loadtest   # sidecar overhead at 1,000 req/s
```

- **Unit:** sketch accuracy and bound coverage under repeated looks. 50 simulated identical-model rollouts stay under 2% false rollbacks, and each bad variant must fail on the expected check. Controller logic runs against a fake client.
- **envtest:** the webhook behind a real API server. A loose rollout is rejected with a reason, a compliant one is accepted, annotation-only updates pass after the policy tightens, and a second policy name is refused.
- **envtest:** the generated CRD schema, CEL rules, defaults, pod-template metadata, the status subresource, and the experimental mirror field are all enforced by a real API server. It also drives a full rollout: deploy, shadow, three steps, promote, then a bad canary rolled back.
- **Local smoke:** all 8 demo variants through the real binaries. `good` and `identical` pass; the 6 bad variants fail.
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

### Sidecar overhead

`make loadtest`: open-loop at 1,000 req/s for 30 s, 5 ms model, capture on every request. The sidecar adds **0.13 ms at p99** (5.85 to 5.98 ms) and drops no events. Measured on one Windows dev machine, client and server on the same host, so treat it as indicative, not per-pod.

## Known limitations

- The canary runs the same replica count as stable.
- Only the current template can be rendered. If someone deletes the old stable Deployment mid-rollout, the rollout stalls instead of recreating it.
- The analyzer keeps sketches in memory. Run it with NATS so a restart can replay the last hour. The NATS path has unit-level code only and has not been run against a NATS server yet.
- The label-free checks will likely miss the subtle 5-point accuracy drop. Delayed labels are planned for that case.

Licensed under Apache 2.0.
