# Changelog

All notable changes to ModelGate are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). While the API is `v1alpha1`,
minor versions may change it; such changes are called out below.

## [Unreleased]

### Added

- Opt-in raw payload capture for debugging (`spec.capture.payloads`). It is
  time-limited (`until`, at most 24 hours ahead), redacts JSONPath fields in
  the sidecar before anything leaves the pod, captures only JSON bodies, and
  never captures headers. The analyzer keeps the last 500 payloads per
  rollout for an hour (`GET /v1/payloads`), and `kubectl modelgate payloads`
  shows them. A `ModelGatePolicy` can forbid payload capture or require
  redaction paths.
- `ModelGatePolicy`: one policy per namespace, named `default`, that teams
  can only tighten. The controller merges it into every rollout and reports
  the result in the `PolicyApplied` condition. A validating webhook rejects
  rollouts looser than the policy; it fails open, and the controller enforces
  the policy either way.
- PostgreSQL audit log of every verdict (with disagreement samples), step
  change, promotion, rollback, pause, resume and manual action, with the
  actor's identity. Writes are idempotent across retried reconciles. When the
  database is unreachable, rollouts hold instead of advancing; rollbacks still
  happen.
- Grafana dashboard, shipped in the Helm chart as a ConfigMap for the Grafana
  sidecar.
- End-to-end tests on kind in CI: a good model promotes, bad models roll
  back, and a chaos run kills the controller and analyzer during a rollout
  and checks that no bad canary is promoted.
- Release pipeline: multi-arch images, SPDX SBOMs, keyless cosign signatures,
  a vulnerability scan, kubectl plugin binaries, and the Helm chart on GHCR.
- Apache 2.0 license.
- Delayed-label accuracy: a `labelAccuracy` check, and an analyzer
  `POST /v1/outcomes` endpoint that joins ground-truth labels with captured
  predictions by request ID. Checks can set their own `minSamples`. The
  benchmark gained a delayed-label mode, and the demo load generator can post
  labels.
- `hack/quickstart.sh`: one command from nothing to a running demo on kind,
  with Prometheus, Grafana, the audit log, traffic and labels. `hack/demo.sh`
  ships broken or good models and shows the outcome and the audit trail.
- `kubectl modelgate status -w` refreshes the status every two seconds.

### Changed

- The NATS sink in the sidecar connects in the background, so a sidecar
  that starts while NATS is down begins publishing once NATS is back.
- A namespace policy's `minSamples` floor also applies to per-check
  `minSamples`, except for label checks.

## [0.1.0]

### Added

- `ModelRollout` CRD and controller: shadow analysis, weighted steps through
  the Gateway API, promotion and rollback, kill switch, leader election, and
  manual actions with the caller's identity.
- Capture sidecar with deterministic sampling, publishing over HTTP or NATS
  JetStream.
- Analyzer with nine checks, sequential testing with an alpha budget spent
  across looks, and Argo Rollouts and Flagger webhook endpoints.
- `kubectl modelgate` plugin, Helm chart, demo model, benchmark and load test.

[Unreleased]: https://github.com/modelgate/modelgate/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/modelgate/modelgate/releases/tag/v0.1.0
