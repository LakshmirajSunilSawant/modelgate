#!/usr/bin/env bash
# Quickstart: from nothing to a running ModelGate demo on kind, timed.
#
#   hack/quickstart.sh          # create cluster, build, install, start demo traffic
#   hack/quickstart.sh down     # stop the background traffic and port-forwards
#   hack/quickstart.sh delete   # also delete the kind cluster
#
# It installs ModelGate, the demo model behind Envoy Gateway, a PostgreSQL
# audit log, Prometheus and Grafana with the ModelGate dashboard, and starts
# 50 req/s of traffic plus delayed ground-truth labels. Rollout stages are
# shortened (shadow 60s, then 20% and 50% for 45s each) so a full rollout
# takes about three minutes. Needs docker, kind, kubectl, helm, and python
# with demo/model/requirements.txt.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
CLUSTER=${CLUSTER:-modelgate}
TAG=${TAG:-dev}
REG=${REG:-ghcr.io/modelgate}
PYTHON=${PYTHON:-python}
SYS=modelgate-system
NS=demo
K="kubectl --context kind-$CLUSTER"
RUN=$ROOT/bin/quickstart
mkdir -p "$RUN"

stop_background() {
  if [ -f "$RUN/pids" ]; then
    while read -r pid; do kill "$pid" 2>/dev/null || true; done <"$RUN/pids"
    rm -f "$RUN/pids"
  fi
  # Windows: background python and kubectl survive their parent shell.
  if command -v powershell >/dev/null 2>&1; then
    powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { \$_.CommandLine -match 'loadgen.py|port-forward.*(18080|18090|3000|9090)' } | ForEach-Object { Stop-Process -Id \$_.ProcessId -Force -ErrorAction SilentlyContinue }" || true
  fi
}

case "${1:-up}" in
  down) stop_background; echo "background traffic and port-forwards stopped"; exit 0 ;;
  delete) stop_background; kind delete cluster --name "$CLUSTER"; exit 0 ;;
  up) ;;
  *) echo "usage: $0 [up|down|delete]"; exit 1 ;;
esac

START=$SECONDS
step() { printf '\n\033[1m[%3ds] %s\033[0m\n' $((SECONDS - START)) "$*"; }

step "kind cluster + Envoy Gateway"
./hack/kind-up.sh "$CLUSTER"

if [ "${SKIP_BUILD:-0}" = 1 ]; then
  step "reusing loaded images (SKIP_BUILD=1)"
else
step "building images"
# One build at a time: parallel BuildKit builds can exhaust a small Docker VM.
for pair in manager:modelgate-controller analyzer:modelgate-analyzer sidecar:modelgate-sidecar; do
  docker build -q --build-arg CMD="${pair%%:*}" -t "$REG/${pair#*:}:$TAG" . >/dev/null
done
docker build -q -t modelgate-demo-model:dev demo/model >/dev/null
go build -o bin/ ./cmd/kubectl-modelgate
for img in modelgate-controller modelgate-analyzer modelgate-sidecar; do
  kind load docker-image "$REG/$img:$TAG" --name "$CLUSTER" >/dev/null
done
kind load docker-image modelgate-demo-model:dev --name "$CLUSTER" >/dev/null
fi

step "installing ModelGate with the PostgreSQL audit log"
$K create namespace $SYS --dry-run=client -o yaml | $K apply -f - >/dev/null
$K apply -f config/samples/postgres.yaml >/dev/null
$K -n $SYS rollout status deploy/modelgate-postgres --timeout=5m >/dev/null
$K apply --server-side --force-conflicts -f config/crd/ >/dev/null
helm --kube-context "kind-$CLUSTER" upgrade --install modelgate charts/modelgate -n $SYS \
  --set image.registry="$REG" --set image.tag="$TAG" --set image.pullPolicy=IfNotPresent \
  --set audit.existingSecret=modelgate-audit --wait --timeout 5m >/dev/null
$K -n $SYS rollout restart deploy/modelgate-controller deploy/modelgate-analyzer >/dev/null
$K apply -f hack/observability.yaml >/dev/null

step "deploying the demo model"
$K create namespace $NS --dry-run=client -o yaml | $K apply -f - >/dev/null
$K -n $NS delete modelgatepolicy --all --ignore-not-found >/dev/null
$K apply -f config/samples/demo.yaml >/dev/null
$K -n $NS patch modelrollout demo-model --type merge -p '{"spec":{
  "strategy":{"shadow":{"duration":"60s","sampleRate":1},
    "steps":[{"weight":20,"pause":"45s"},{"weight":50,"pause":"45s"}]},
  "analysis":{"minSamples":300,"interval":"10s","checks":[
    {"name":"agreement","type":"agreementRate","min":0.97,"phase":"shadow"},
    {"name":"pred-shift","type":"psi","max":0.10},
    {"name":"score-shape","type":"ks","max":0.10},
    {"name":"valid","type":"validity","max":0},
    {"name":"latency","type":"p99Ratio","max":1.20},
    {"name":"errors","type":"errorRate","max":0.01},
    {"name":"accuracy","type":"labelAccuracy","max":0.02,"minSamples":100}]}}}' >/dev/null
for d in modelgate-controller modelgate-analyzer modelgate-postgres prometheus grafana; do
  $K -n $SYS rollout status deploy/$d --timeout=5m >/dev/null
done
deadline=$((SECONDS + 300))
until [ "$($K -n $NS get modelrollout demo-model -o jsonpath='{.status.phase}' 2>/dev/null)" = Healthy ]; do
  [ $SECONDS -lt $deadline ] || { echo "demo rollout did not become Healthy"; exit 1; }
  sleep 3
done
$K -n $NS wait --for=condition=Available deploy -l modelgate.dev/rollout=demo-model --timeout=5m >/dev/null 2>&1 || true
# Envoy Gateway creates the gateway's proxy Deployment once the Gateway
# exists. (Its Programmed condition stays False on kind: no LoadBalancer.)
GW_SEL=gateway.envoyproxy.io/owning-gateway-name=demo
deadline=$((SECONDS + 300))
until [ -n "$($K -n envoy-gateway-system get deploy -l $GW_SEL -o name 2>/dev/null)" ]; do
  [ $SECONDS -lt $deadline ] || { echo "Envoy proxy for gateway demo was not created"; exit 1; }
  sleep 3
done
$K -n envoy-gateway-system wait --for=condition=Available deploy -l $GW_SEL --timeout=5m >/dev/null

step "starting traffic and delayed labels"
stop_background
svc=$($K -n envoy-gateway-system get svc -o name \
  -l gateway.envoyproxy.io/owning-gateway-name=demo,gateway.envoyproxy.io/owning-gateway-namespace=$NS | head -1)
forward() { # forward NAME ARGS...: keep a port-forward up in the background
  local name=$1; shift
  ( while true; do $K "$@" >>"$RUN/$name.log" 2>&1; sleep 1; done ) &
  echo $! >>"$RUN/pids"
}
forward gateway -n envoy-gateway-system port-forward "$svc" 18080:80
forward analyzer -n $SYS port-forward svc/modelgate-analyzer 18090:80
forward grafana -n $SYS port-forward svc/grafana 3000:3000
forward prometheus -n $SYS port-forward svc/prometheus 9090:9090
deadline=$((SECONDS + 120))
until curl -sf -o /dev/null http://localhost:18090/healthz && curl -sf -o /dev/null http://localhost:3000/api/health     && curl -s -o /dev/null http://localhost:18080/; do
  [ $SECONDS -lt $deadline ] || { echo "port-forwards did not come up; see $RUN/*.log"; exit 1; }
  sleep 2
done
"$PYTHON" demo/model/loadgen.py --url http://localhost:18080/predict --rps 50 --workers 32 --duration 86400 \
  --outcomes-url http://localhost:18090/v1/outcomes --label-delay 20 --label-rate 0.5 >"$RUN/loadgen.log" 2>&1 &
echo $! >>"$RUN/pids"

step "ready in $((SECONDS - START))s"
cat <<EOF

  Grafana dashboard   http://localhost:3000/d/modelgate
  Live status         bin/kubectl-modelgate status demo-model -n $NS -w
  Ship a broken model hack/demo.sh bad swapped
  Ship a good model   hack/demo.sh good
  Audit log           hack/demo.sh audit
  Stop traffic        hack/quickstart.sh down

EOF
