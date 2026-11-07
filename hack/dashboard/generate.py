"""Generates charts/modelgate/dashboards/modelgate.json.

  python hack/dashboard/generate.py charts/modelgate/dashboards/modelgate.json
"""
import json
import sys

DS = {"type": "prometheus", "uid": "${datasource}"}
SEL = 'namespace=~"$namespace", rollout=~"$rollout"'
panels = []
pid = 0


def panel(title, kind, exprs, x, y, w, h, unit=None, desc="", legend=None, extra=None):
    global pid
    pid += 1
    p = {
        "id": pid, "title": title, "type": kind, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS}
                    for i, (e, l) in enumerate(exprs)],
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {},
    }
    if unit:
        p["fieldConfig"]["defaults"]["unit"] = unit
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "table", "placement": "bottom", "calcs": legend or ["lastNotNull"]},
                        "tooltip": {"mode": "multi"}}
    if extra:
        extra(p)
    panels.append(p)


def row(title, y):
    global pid
    pid += 1
    panels.append({"id": pid, "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


def phase_table(p):
    p["options"] = {"showHeader": True}
    p["targets"][0]["format"] = "table"
    p["targets"][0]["instant"] = True
    p["transformations"] = [
        {"id": "filterByValue", "options": {"filters": [{"fieldName": "Value", "config": {"id": "equal", "options": {"value": 1}}}],
                                            "type": "include", "match": "any"}},
        {"id": "organize", "options": {"excludeByName": {"Time": True, "Value": True, "__name__": True, "instance": True,
                                                         "job": True, "pod": True, "container": True, "endpoint": True, "service": True}}},
    ]
    p["fieldConfig"]["defaults"]["mappings"] = []
    p["fieldConfig"]["overrides"] = [{
        "matcher": {"id": "byName", "options": "phase"},
        "properties": [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                       {"id": "mappings", "value": [{"type": "value", "options": {
                           "Healthy": {"color": "green", "index": 0}, "Progressing": {"color": "blue", "index": 1},
                           "Paused": {"color": "orange", "index": 2}, "Degraded": {"color": "red", "index": 3}}}]}],
    }]


def stat_colored(color):
    def f(p):
        p["options"] = {"colorMode": "background", "graphMode": "none", "reduceOptions": {"calcs": ["lastNotNull"]}}
        p["fieldConfig"]["defaults"]["color"] = {"mode": "fixed", "fixedColor": color}
    return f


y = 0
row("Rollouts", y); y += 1
panel("Current phase", "table", [(f'max by (namespace, rollout, phase) (modelgate_rollout_phase{{{SEL}}})', "")],
      0, y, 9, 7, desc="One row per rollout with its current phase.", extra=phase_table)
panel("Promotions (range)", "stat", [(f'sum(increase(modelgate_rollout_decisions_total{{{SEL}, decision="promote"}}[$__range]))', "")],
      9, y, 5, 7, extra=stat_colored("green"))
panel("Rollbacks (range)", "stat", [(f'sum(increase(modelgate_rollout_decisions_total{{{SEL}, decision="rollback"}}[$__range]))', "")],
      14, y, 5, 7, extra=stat_colored("red"))
panel("Reconcile errors (range)", "stat", [('sum(increase(controller_runtime_reconcile_errors_total{controller="modelrollout"}[$__range]))', "")],
      19, y, 5, 7, desc="Failed reconcile passes. A steady rise usually means the audit log, policy, or HTTPRoute cannot be read.",
      extra=stat_colored("orange"))
y += 7
panel("Canary weight", "timeseries", [(f'max by (namespace, rollout) (modelgate_rollout_canary_weight{{{SEL}}})', "{{namespace}}/{{rollout}}")],
      0, y, 12, 8, unit="percent", desc="Percent of live traffic sent to the canary. Shadow stages show 0.")
panel("Verdicts", "timeseries", [(f'sum by (decision) (increase(modelgate_rollout_decisions_total{{{SEL}, decision=~"pass|fail|inconclusive"}}[$__rate_interval]))', "{{decision}}")],
      12, y, 12, 8, legend=["sum"], desc="Analysis looks by outcome.",
      extra=lambda p: p["fieldConfig"].update({"defaults": {"custom": {"drawStyle": "bars", "stacking": {"mode": "normal"}}}}))
y += 8
row("Quality checks", y); y += 1
panel("Check values", "timeseries", [(f'max by (namespace, rollout, check) (modelgate_rollout_check_value{{{SEL}}})', "{{rollout}} {{check}}")],
      0, y, 24, 9, desc="Latest value of each check as seen by the controller. Compare with the thresholds on the rollout status.")
y += 9
row("Data path", y); y += 1
panel("Analyzer ingest", "timeseries", [("sum(rate(modelgate_analyzer_events_total[$__rate_interval]))", "prediction events/s"),
                                     ("sum(rate(modelgate_analyzer_outcomes_scored_total[$__rate_interval]))", "predictions scored against labels/s")],
      0, y, 8, 8, unit="reqps")
panel("Sidecar requests", "timeseries", [("sum(rate(modelgate_sidecar_requests_total[$__rate_interval]))", "requests/s")],
      8, y, 8, 8, unit="reqps")
panel("Sidecar capture outcomes", "timeseries", [("sum by (outcome) (rate(modelgate_sidecar_captured_total[$__rate_interval]))", "{{outcome}}")],
      16, y, 8, 8, unit="reqps", desc="Anything other than published means events are being dropped and rollouts may hold for lack of samples.")

dash = {
    "title": "ModelGate", "uid": "modelgate", "tags": ["modelgate", "kubernetes"], "schemaVersion": 39, "version": 1,
    "editable": True, "refresh": "30s", "time": {"from": "now-3h", "to": "now"},
    "annotations": {"list": []},
    "templating": {"list": [
        {"name": "datasource", "type": "datasource", "query": "prometheus", "label": "Data source"},
        {"name": "namespace", "type": "query", "datasource": DS, "label": "Namespace", "includeAll": True, "multi": True,
         "current": {"text": "All", "value": "$__all"}, "refresh": 2,
         "query": {"query": "label_values(modelgate_rollout_phase, namespace)", "refId": "ns"}},
        {"name": "rollout", "type": "query", "datasource": DS, "label": "Rollout", "includeAll": True, "multi": True,
         "current": {"text": "All", "value": "$__all"}, "refresh": 2,
         "query": {"query": 'label_values(modelgate_rollout_phase{namespace=~"$namespace"}, rollout)', "refId": "ro"}},
    ]},
    "panels": panels,
}
with open(sys.argv[1], "w", encoding="utf-8", newline="") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
