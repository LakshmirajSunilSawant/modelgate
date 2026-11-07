#!/usr/bin/env bash
# Demo scenes for a recording, run after hack/quickstart.sh.
#
#   hack/demo.sh bad [variant]   ship a broken model (default swapped) and watch it roll back
#   hack/demo.sh good            ship a retrained, equally good model and watch it promote
#   hack/demo.sh audit           show the audit log for the demo rollout
#   hack/demo.sh variants        list the demo model variants
#
# Broken variants: swapped (labels flipped), scaling (preprocessing bug),
# stale (trained on old data), accuracy-drop, nan (NaN outputs), slow (2x latency).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
CLUSTER=${CLUSTER:-modelgate}
NS=demo
K="kubectl --context kind-$CLUSTER"
PLUGIN="$ROOT/bin/kubectl-modelgate"
[ -x "$PLUGIN" ] || PLUGIN="$ROOT/bin/kubectl-modelgate.exe"

field() { $K -n $NS get modelrollout demo-model -o jsonpath="{$1}"; }

ship() { # ship VARIANT: change the model in spec.template, which starts a rollout
  local variant=$1
  echo "\$ kubectl -n $NS patch modelrollout demo-model  # MODEL_VARIANT=$variant"
  $K -n $NS patch modelrollout demo-model --type json -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/env\",
    \"value\":[{\"name\":\"MODEL_VARIANT\",\"value\":\"$variant\"},{\"name\":\"BUILD\",\"value\":\"$(date +%s)\"}]}]"
}

follow() { # follow until the rollout settles, then print the final status
  local stable_before=$1 start=$SECONDS
  sleep 3
  while true; do
    clear 2>/dev/null || printf '\033[H\033[2J'
    printf 'elapsed %ds\n\n' $((SECONDS - start))
    "$PLUGIN" status demo-model -n $NS
    phase=$(field .status.phase); canary=$(field .status.canaryRevision); stable=$(field .status.stableRevision)
    if [ -z "$canary" ] && { [ "$phase" = Degraded ] || [ "$stable" != "$stable_before" ]; }; then
      break
    fi
    sleep 2
  done
  echo
  echo "\$ kubectl -n $NS get events --field-selector involvedObject.name=demo-model"
  $K -n $NS get events --field-selector involvedObject.name=demo-model --sort-by=.lastTimestamp \
    -o custom-columns=TYPE:.type,REASON:.reason,MESSAGE:.message | tail -6
}

case "${1:-}" in
  bad)
    before=$(field .status.stableRevision)
    ship "${2:-swapped}"
    follow "$before"
    ;;
  good)
    before=$(field .status.stableRevision)
    ship identical
    follow "$before"
    ;;
  audit)
    $K -n modelgate-system exec deploy/modelgate-postgres -- psql -U modelgate -d modelgate -c \
      "SELECT to_char(recorded_at, 'HH24:MI:SS') AS time, action, actor, canary_revision AS canary,
              canary_weight AS weight, left(reason, 70) AS reason
       FROM modelgate_audit WHERE namespace = '$NS' AND rollout = 'demo-model' AND action <> 'Verdict'
       ORDER BY recorded_at DESC LIMIT 20"
    ;;
  variants)
    sed -n '/^# Broken variants/,/^set /p' "$0" | sed '$d'
    ;;
  *)
    sed -n '2,8p' "$0"
    exit 1
    ;;
esac
