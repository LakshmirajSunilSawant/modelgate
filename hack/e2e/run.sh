#!/usr/bin/env bash
# End-to-end test on kind. It installs ModelGate with NATS JetStream and the
# PostgreSQL audit log, drives load through Envoy Gateway, and runs rollouts
# of demo model variants: good ones must promote, bad ones must roll back, and
# a bad canary must never become stable, with or without chaos.
#
#   hack/e2e/run.sh                         # full run on a new or existing cluster
#   SETUP=0 hack/e2e/run.sh                 # reuse an installed cluster
#   CHAOS=mesh hack/e2e/run.sh              # Chaos Mesh kills controller, analyzer, NATS
#   CHAOS=kubectl hack/e2e/run.sh           # same kills with kubectl, no Chaos Mesh
#   SCENARIOS="swapped:rollback" hack/e2e/run.sh
#   RPS=0 REPLAY=1 hack/e2e/run.sh          # no live traffic: replayed requests drive analysis
#   PROTOCOL=grpc hack/e2e/run.sh           # KServe v2 gRPC model behind a GRPCRoute
#   PROTOCOL=v2 hack/e2e/run.sh             # KServe v2 REST
#
# Needs docker, kind, kubectl, helm, and python with demo/model/requirements.txt.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
CLUSTER=${CLUSTER:-modelgate}
TAG=${TAG:-e2e}
REG=${REG:-ghcr.io/modelgate}
SYS=modelgate-system
NS=demo
SETUP=${SETUP:-1}
CHAOS=${CHAOS:-off}
REPLAY=${REPLAY:-0}
PROTOCOL=${PROTOCOL:-json}
RPS=${RPS:-50}
TIMEOUT=${TIMEOUT:-900}
SCENARIOS=${SCENARIOS:-"identical:promote swapped:rollback nan:rollback scaling:rollback"}
NATS_CHART=${NATS_CHART:-2.15.1}
CHAOS_MESH_CHART=${CHAOS_MESH_CHART:-2.8.6}
PORT=${PORT:-18080}
PYTHON=${PYTHON:-python}
K="kubectl --context kind-$CLUSTER"
LOGDIR=${LOGDIR:-$ROOT/bin/e2e}
mkdir -p "$LOGDIR"

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done
  if [ "$CHAOS" = mesh ]; then $K delete -f hack/e2e/chaos.yaml --ignore-not-found >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

log() { printf '\n[e2e %s] %s\n' "$(date +%H:%M:%S)" "$*"; }
fail() { log "FAIL: $*"; dump; exit 1; }

dump() {
  $K -n $NS get modelrollout demo-model -o yaml > "$LOGDIR/rollout.yaml" 2>&1 || true
  $K -n $NS get events --sort-by=.lastTimestamp > "$LOGDIR/events.txt" 2>&1 || true
  $K -n $NS get httproute,grpcroute,gateway,service -o yaml > "$LOGDIR/routes.yaml" 2>&1 || true
  $K -n $NS logs -l modelgate.dev/rollout=demo-model --all-containers --tail=200 --prefix > "$LOGDIR/models.log" 2>&1 || true
  $K -n envoy-gateway-system logs deploy/envoy-gateway --tail=300 > "$LOGDIR/envoy-gateway.log" 2>&1 || true
  $K -n $SYS logs -l app.kubernetes.io/name=modelgate-controller --tail=500 --prefix > "$LOGDIR/controller.log" 2>&1 || true
  $K -n $SYS logs -l app.kubernetes.io/name=modelgate-analyzer --tail=500 > "$LOGDIR/analyzer.log" 2>&1 || true
  $K get pods -A > "$LOGDIR/pods.txt" 2>&1 || true
  log "diagnostics written to $LOGDIR"
}

setup() {
  log "cluster and Envoy Gateway"
  ./hack/kind-up.sh "$CLUSTER"

  log "build and load images"
  for pair in manager:modelgate-controller analyzer:modelgate-analyzer sidecar:modelgate-sidecar; do
    docker build -q --build-arg CMD="${pair%%:*}" -t "$REG/${pair#*:}:$TAG" . >/dev/null
    kind load docker-image "$REG/${pair#*:}:$TAG" --name "$CLUSTER"
  done
  docker build -q -t modelgate-demo-model:dev demo/model >/dev/null
  kind load docker-image modelgate-demo-model:dev --name "$CLUSTER"

  $K create namespace $SYS --dry-run=client -o yaml | $K apply -f -
  log "NATS JetStream"
  helm repo add nats https://nats-io.github.io/k8s/helm/charts >/dev/null 2>&1 || true
  helm --kube-context "kind-$CLUSTER" upgrade --install nats nats/nats --version "$NATS_CHART" -n $SYS \
    --set config.jetstream.enabled=true --set config.jetstream.fileStore.pvc.size=1Gi \
    --set natsBox.enabled=false --wait --timeout 5m

  log "PostgreSQL audit log"
  $K apply -f config/samples/postgres.yaml
  $K -n $SYS rollout status deploy/modelgate-postgres --timeout=3m

  log "ModelGate"
  $K apply --server-side --force-conflicts -f config/crd/
  helm --kube-context "kind-$CLUSTER" upgrade --install modelgate charts/modelgate -n $SYS \
    --set image.registry="$REG" --set image.tag="$TAG" --set image.pullPolicy=IfNotPresent \
    --set nats.url=nats://nats.$SYS.svc:4222 --set audit.existingSecret=modelgate-audit --wait --timeout 5m
  # The tag is reused across runs, so restart to pick up freshly loaded images.
  $K -n $SYS rollout restart deploy/modelgate-controller deploy/modelgate-analyzer
  $K -n $SYS rollout status deploy/modelgate-controller --timeout=3m
  $K -n $SYS rollout status deploy/modelgate-analyzer --timeout=3m

  log "demo rollout"
  $K create namespace $NS --dry-run=client -o yaml | $K apply -f -
  # A namespace policy from manual testing would reject the e2e settings.
  $K -n $NS delete modelgatepolicy --all --ignore-not-found
  if [ "$PROTOCOL" = grpc ]; then
    $K apply -f config/samples/demo-grpc.yaml
  else
    $K apply -f config/samples/demo.yaml
  fi
}

# Where the load generator sends requests through the gateway.
load_url() {
  case "$PROTOCOL" in
    grpc) echo "localhost:$PORT" ;;
    v2) echo "http://localhost:$PORT/v2/models/demo/infer" ;;
    *) echo "http://localhost:$PORT/predict" ;;
  esac
}

# KServe v2 REST: same demo model, read through output tensors instead of
# JSON paths. Only the sidecars change, so no rollout starts.
protocol_config() {
  [ "$PROTOCOL" = v2 ] || return 0
  log "KServe v2 REST protocol"
  $K -n $NS patch modelrollout demo-model --type merge -p '{"spec":{"output":{"protocol":"kserveV2",
    "labelPath":null,"scorePath":null,"labelTensor":{"name":"label"},"scoreTensor":{"name":"confidence"}}}}'
  sleep 5
  for d in $($K -n $NS get deploy -l modelgate.dev/rollout=demo-model -o name); do
    $K -n $NS rollout status "$d" --timeout=3m
  done
}

# Short stages so the suite fits in CI. Evidence requirements stay real:
# 300 samples per side, sequential test on every look. The first step is 20%
# rather than 5% so the canary collects its samples in seconds, not minutes.
fast_config() {
  $K -n $NS patch modelrollout demo-model --type merge -p '{"spec":{
    "strategy":{"shadow":{"duration":"60s","sampleRate":1},
      "steps":[{"weight":20,"pause":"45s"},{"weight":50,"pause":"45s"}]},
    "analysis":{"minSamples":300,"interval":"10s"}}}'
}

# Replay: record the demo dataset as JSON request bodies in a ConfigMap and
# have ModelGate send them to stable and canary during analysis.
replay_config() {
  [ "$REPLAY" = 1 ] || return 0
  log "replay of recorded requests"
  local path=/predict
  [ "$PROTOCOL" = v2 ] && path=/v2/models/demo/infer
  PROTOCOL=$PROTOCOL "$PYTHON" -c 'import json, os; from sklearn.datasets import load_breast_cancer as d
for r in d().data.tolist():
    f = [round(v, 5) for v in r]
    v2 = {"inputs": [{"name": "features", "datatype": "FP64", "shape": [1, len(f)], "data": f}]}
    print(json.dumps(v2 if os.environ["PROTOCOL"] == "v2" else {"features": f}))' >"$LOGDIR/requests.jsonl"
  # Server-side apply: the file is too large for the last-applied annotation.
  $K -n $NS create configmap demo-model-replay --from-file=requests.jsonl="$LOGDIR/requests.jsonl" \
    --dry-run=client -o yaml | $K apply --server-side --force-conflicts -f -
  $K -n $NS patch modelrollout demo-model --type merge -p '{"spec":{"strategy":{"replay":{
    "configMapRef":{"name":"demo-model-replay"},"path":"'"$path"'","ratePerSecond":40}}}}'
}

field() { $K -n $NS get modelrollout demo-model -o jsonpath="{$1}" 2>/dev/null || true; }

wait_settled() {
  local deadline=$((SECONDS + TIMEOUT))
  until [ "$(field .status.phase)" = Healthy ] || [ "$(field .status.phase)" = Degraded ]; do
    [ $SECONDS -lt $deadline ] || fail "rollout did not settle: $(field .status.message)"
    sleep 5
  done
}

start_load() {
  local svc="" deadline=$((SECONDS + 120))
  # Envoy Gateway creates the Service a few seconds after the Gateway.
  until [ -n "$svc" ]; do
    [ $SECONDS -lt $deadline ] || fail "no Envoy service for gateway demo"
    sleep 2
    svc=$($K -n envoy-gateway-system get svc -o name \
      -l gateway.envoyproxy.io/owning-gateway-name=demo,gateway.envoyproxy.io/owning-gateway-namespace=$NS | head -1)
  done
  # Restarted if it drops. The trap stops the kubectl child with the loop.
  (
    trap 'kill $(jobs -p) 2>/dev/null; exit 0' TERM
    while true; do
      $K -n envoy-gateway-system port-forward "$svc" "$PORT:80" >>"$LOGDIR/port-forward.log" 2>&1 &
      wait $! || true
      sleep 1
    done
  ) &
  pids+=($!)
  local deadline=$((SECONDS + 90))
  until "$PYTHON" demo/model/loadgen.py --check --protocol "$PROTOCOL" --url "$(load_url)" >/dev/null 2>&1; do
    [ $SECONDS -lt $deadline ] || fail "gateway not serving $PROTOCOL on localhost:$PORT"
    sleep 2
  done
  if [ "$RPS" = 0 ]; then
    log "no live load"
    return
  fi
  "$PYTHON" demo/model/loadgen.py --protocol "$PROTOCOL" --url "$(load_url)" --rps "$RPS" --workers 32 --duration 100000 \
    >"$LOGDIR/loadgen.log" 2>&1 &
  pids+=($!)
  log "load running at $RPS rps ($PROTOCOL)"
}

start_chaos() {
  case "$CHAOS" in
    off) return ;;
    mesh)
      log "Chaos Mesh"
      helm repo add chaos-mesh https://charts.chaos-mesh.org >/dev/null 2>&1 || true
      helm --kube-context "kind-$CLUSTER" upgrade --install chaos-mesh chaos-mesh/chaos-mesh --version "$CHAOS_MESH_CHART" \
        -n chaos-mesh --create-namespace --set chaosDaemon.runtime=containerd \
        --set chaosDaemon.socketPath=/run/containerd/containerd.sock --set dashboard.create=false \
        --set controllerManager.replicaCount=1 --wait --timeout 5m
      $K apply -f hack/e2e/chaos.yaml
      ;;
    kubectl)
      log "kubectl pod killer"
      (
        targets=(app.kubernetes.io/name=modelgate-controller app.kubernetes.io/name=modelgate-analyzer app.kubernetes.io/name=nats)
        i=0
        while true; do
          sleep 40
          sel=${targets[$((i % 3))]}
          pod=$($K -n $SYS get pods -l "$sel" -o name | shuf -n1 2>/dev/null || $K -n $SYS get pods -l "$sel" -o name | head -1)
          [ -n "$pod" ] && echo "$(date +%T) killing $pod" && $K -n $SYS delete "$pod" --wait=false --grace-period=0 --force
          i=$((i + 1))
        done
      ) >>"$LOGDIR/chaos.log" 2>&1 &
      pids+=($!)
      ;;
    *) fail "unknown CHAOS=$CHAOS" ;;
  esac
}

# rollout VARIANT EXPECT: start a rollout of the variant and wait for the
# expected outcome. A bad variant becoming stable fails immediately.
promotes=0
rollbacks=0
rollout() {
  local variant=$1 expect=$2 run
  run="$variant-$(date +%s)"
  local stable_before
  stable_before=$(field .status.stableRevision)
  log "rollout $variant, expecting $expect"
  $K -n $NS patch modelrollout demo-model --type json -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/env\",
    \"value\":[{\"name\":\"MODEL_VARIANT\",\"value\":\"$variant\"},{\"name\":\"E2E_RUN\",\"value\":\"$run\"}]}]"
  local deadline=$((SECONDS + TIMEOUT)) started=0 last=""
  while [ $SECONDS -lt $deadline ]; do
    local phase stable canary msg
    phase=$(field .status.phase); stable=$(field .status.stableRevision)
    canary=$(field .status.canaryRevision); msg=$(field .status.message)
    [ -n "$canary" ] && started=1
    if [ "$msg" != "$last" ]; then echo "  $(date +%T) $phase: $msg"; last=$msg; fi
    if [ -n "$stable" ] && [ "$stable" != "$stable_before" ]; then
      [ "$expect" = promote ] || fail "UNSAFE PROMOTION: bad variant $variant became stable ($stable)"
      [ "$phase" = Healthy ] && { promotes=$((promotes + 1)); log "PASS $variant promoted"; return; }
    fi
    if [ $started = 1 ] && [ "$phase" = Degraded ] && [ -z "$canary" ]; then
      [ "$expect" = rollback ] || fail "good variant $variant rolled back: $msg"
      rollbacks=$((rollbacks + 1))
      log "PASS $variant rolled back"
      return
    fi
    sleep 5
  done
  fail "$variant: no outcome within ${TIMEOUT}s (last: $last)"
}

check_audit() {
  log "audit log"
  local rows
  rows=$($K -n $SYS exec deploy/modelgate-postgres -- psql -U modelgate -d modelgate -tA -F' ' -c \
    "select action, count(*) from modelgate_audit where namespace='$NS' and rollout='demo-model' group by action order by action")
  echo "$rows" | sed 's/^/  /'
  local p r
  p=$(echo "$rows" | awk '$1=="Promoted"{print $2}'); r=$(echo "$rows" | awk '$1=="RolledBack"{print $2}')
  [ "${p:-0}" -ge "$promotes" ] || fail "audit log has ${p:-0} promotions, expected at least $promotes"
  [ "${r:-0}" -ge "$rollbacks" ] || fail "audit log has ${r:-0} rollbacks, expected at least $rollbacks"
}

[ "$SETUP" = 1 ] && setup
fast_config
protocol_config
replay_config
start_load
wait_settled # a rollout left over from an earlier run finishes first
start_chaos
for s in $SCENARIOS; do
  rollout "${s%%:*}" "${s#*:}"
done
check_audit
log "all scenarios passed (chaos=$CHAOS): $promotes promoted, $rollbacks rolled back"
