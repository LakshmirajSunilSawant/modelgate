package policy

import (
	"strings"
	"testing"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

func f(v float64) *float64 { return &v }
func i32(v int32) *int32   { return &v }
func i64(v int64) *int64   { return &v }

func spec() *mgv1.ModelRolloutSpec {
	return &mgv1.ModelRolloutSpec{
		Strategy: mgv1.StrategySpec{Steps: []mgv1.Step{{Weight: 20}, {Weight: 50}}},
		Analysis: mgv1.AnalysisSpec{
			MinSamples: 500, FailureLimit: 5, Alpha: 0.05,
			Checks: []mgv1.CheckSpec{
				{Name: "agree", Type: "agreementRate", Min: f(0.90), Phase: "shadow"},
				{Name: "errors", Type: "errorRate", Max: f(0.001)}, // already stricter
				{Name: "lat", Type: "p99Ratio"},                    // default 1.2
			},
		},
	}
}

var pol = &mgv1.ModelGatePolicySpec{
	Checks: []mgv1.CheckSpec{
		{Name: "agreement", Type: "agreementRate", Min: f(0.97), Phase: "shadow"},
		{Name: "errors", Type: "errorRate", Max: f(0.01)},
		{Name: "latency", Type: "p99Ratio", Max: f(1.1)},
		{Name: "valid", Type: "validity", Max: f(0)},
	},
	MinSamples: i64(2000), MaxFailureLimit: i32(2), MaxAlpha: f(0.01), MaxFirstStepWeight: i32(10),
}

func TestApplyTightensAndAdds(t *testing.T) {
	s := spec()
	res := Apply(pol, s)
	a := s.Analysis
	if *a.Checks[0].Min != 0.97 {
		t.Errorf("agreement min %v, want 0.97", *a.Checks[0].Min)
	}
	if *a.Checks[1].Max != 0.001 {
		t.Errorf("stricter rollout threshold was loosened to %v", *a.Checks[1].Max)
	}
	if a.Checks[2].Max == nil || *a.Checks[2].Max != 1.1 {
		t.Errorf("default-threshold check not tightened: %+v", a.Checks[2])
	}
	if len(a.Checks) != 4 || a.Checks[3].Name != "policy-valid" {
		t.Errorf("required check not added: %+v", a.Checks)
	}
	if a.MinSamples != 2000 || a.FailureLimit != 2 || a.Alpha != 0.01 || s.Strategy.Steps[0].Weight != 10 {
		t.Errorf("scalars not tightened: %+v first step %d", a, s.Strategy.Steps[0].Weight)
	}
	if len(res.Tightened) != 6 || len(res.Added) != 1 {
		t.Errorf("tightened %d added %d:\n%s", len(res.Tightened), len(res.Added), strings.Join(append(res.Tightened, res.Added...), "\n"))
	}
}

func TestApplyCompliantRolloutUnchanged(t *testing.T) {
	s := spec()
	Apply(pol, s)
	again := Apply(pol, s)
	if again.Changed() {
		t.Errorf("second apply changed: %+v", again)
	}
}

// A rollout check limited to one phase does not satisfy a policy check that
// covers both phases.
func TestNarrowerPhaseDoesNotSatisfy(t *testing.T) {
	s := &mgv1.ModelRolloutSpec{Analysis: mgv1.AnalysisSpec{Checks: []mgv1.CheckSpec{
		{Name: "psi-shadow", Type: "psi", Max: f(0.05), Phase: "shadow"},
	}}}
	res := Apply(&mgv1.ModelGatePolicySpec{Checks: []mgv1.CheckSpec{{Name: "psi", Type: "psi", Max: f(0.1)}}}, s)
	if len(res.Added) != 1 || len(s.Analysis.Checks) != 2 {
		t.Errorf("policy check not added alongside narrower one: %+v", s.Analysis.Checks)
	}
}

func TestNilPolicy(t *testing.T) {
	if Apply(nil, spec()).Changed() {
		t.Error("nil policy changed the rollout")
	}
}
