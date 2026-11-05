package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

var (
	phaseGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "modelgate_rollout_phase", Help: "1 for the rollout's current phase, 0 otherwise.",
	}, []string{"namespace", "rollout", "phase"})
	weightGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "modelgate_rollout_canary_weight", Help: "Percent of live traffic sent to the canary.",
	}, []string{"namespace", "rollout"})
	decisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "modelgate_rollout_decisions_total", Help: "Verdicts and rollout decisions (pass, fail, inconclusive, promote, rollback).",
	}, []string{"namespace", "rollout", "decision"})
	checkValue = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "modelgate_rollout_check_value", Help: "Latest value of each quality check.",
	}, []string{"namespace", "rollout", "check"})
)

func init() {
	metrics.Registry.MustRegister(phaseGauge, weightGauge, decisions, checkValue)
}

var allPhases = []mgv1.Phase{mgv1.PhaseHealthy, mgv1.PhaseProgressing, mgv1.PhasePaused, mgv1.PhaseDegraded}

func recordMetrics(mr *mgv1.ModelRollout) {
	for _, p := range allPhases {
		v := 0.0
		if mr.Status.Phase == p {
			v = 1
		}
		phaseGauge.WithLabelValues(mr.Namespace, mr.Name, string(p)).Set(v)
	}
	weightGauge.WithLabelValues(mr.Namespace, mr.Name).Set(float64(planFor(mr).canaryWeight))
}
