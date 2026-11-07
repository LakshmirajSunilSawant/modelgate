// Package policy merges a namespace's ModelGatePolicy into ModelRollouts.
// The same merge runs in the controller (enforcement) and the validating
// webhook (feedback), so they can never disagree.
package policy

import (
	"fmt"
	"strconv"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/analysis"
)

// Result describes what applying a policy changed.
type Result struct {
	// Tightened lists rollout settings that were looser than the policy and
	// were raised to it. The webhook rejects rollouts with any.
	Tightened []string
	// Added lists required checks the rollout did not declare.
	Added []string
}

// Changed reports whether the policy altered the rollout.
func (r Result) Changed() bool { return len(r.Tightened)+len(r.Added) > 0 }

// Apply raises spec to the policy floor in place. A nil policy is a no-op.
func Apply(p *mgv1.ModelGatePolicySpec, spec *mgv1.ModelRolloutSpec) Result {
	var res Result
	if p == nil {
		return res
	}
	a := &spec.Analysis
	for _, pc := range p.Checks {
		pv := value(pc)
		matched := false
		for i := range a.Checks {
			rc := &a.Checks[i]
			if rc.Type != pc.Type || !covers(rc.Phase, pc.Phase) {
				continue
			}
			matched = true
			if rv := value(*rc); looser(pc.Type, rv, pv) {
				res.Tightened = append(res.Tightened, fmt.Sprintf("check %s (%s): %s %s is looser than policy %s",
					rc.Name, rc.Type, field(pc.Type), fmtf(rv), fmtf(pv)))
				set(rc, pv)
			}
		}
		if !matched {
			added := pc
			added.Name = "policy-" + pc.Name
			a.Checks = append(a.Checks, added)
			res.Added = append(res.Added, fmt.Sprintf("check %s (%s) required by policy", added.Name, pc.Type))
		}
	}
	if p.MinSamples != nil && a.MinSamples < *p.MinSamples {
		res.Tightened = append(res.Tightened, fmt.Sprintf("minSamples %d is below policy minimum %d", a.MinSamples, *p.MinSamples))
		a.MinSamples = *p.MinSamples
	}
	// A per-check minSamples must not undercut the floor either. Label checks
	// count ground-truth labels, which are far scarcer, so they are exempt.
	if p.MinSamples != nil {
		for i := range a.Checks {
			c := &a.Checks[i]
			if c.MinSamples != nil && *c.MinSamples < *p.MinSamples && c.Type != analysis.CheckLabelAccuracy {
				res.Tightened = append(res.Tightened, fmt.Sprintf("check %s minSamples %d is below policy minimum %d", c.Name, *c.MinSamples, *p.MinSamples))
				v := *p.MinSamples
				c.MinSamples = &v
			}
		}
	}
	if p.MaxFailureLimit != nil {
		limit := a.FailureLimit
		if limit <= 0 {
			limit = 2
		}
		if limit > *p.MaxFailureLimit {
			res.Tightened = append(res.Tightened, fmt.Sprintf("failureLimit %d is above policy maximum %d", limit, *p.MaxFailureLimit))
			a.FailureLimit = *p.MaxFailureLimit
		}
	}
	if p.MaxAlpha != nil {
		alpha := a.Alpha
		if alpha <= 0 {
			alpha = analysis.DefaultAlpha
		}
		if alpha > *p.MaxAlpha {
			res.Tightened = append(res.Tightened, fmt.Sprintf("alpha %s is above policy maximum %s", fmtf(alpha), fmtf(*p.MaxAlpha)))
			a.Alpha = *p.MaxAlpha
		}
	}
	if steps := spec.Strategy.Steps; p.MaxFirstStepWeight != nil && len(steps) > 0 && steps[0].Weight > *p.MaxFirstStepWeight {
		res.Tightened = append(res.Tightened, fmt.Sprintf("first step weight %d%% is above policy maximum %d%%", steps[0].Weight, *p.MaxFirstStepWeight))
		steps[0].Weight = *p.MaxFirstStepWeight
	}
	return res
}

// covers reports whether a rollout check in phase rp runs everywhere a
// policy check in phase pp must run.
func covers(rp, pp string) bool { return rp == "" || rp == pp }

// value returns a check's effective threshold, falling back to the engine default.
func value(c mgv1.CheckSpec) float64 {
	p := c.Max
	if analysis.HigherIsStricter(c.Type) {
		p = c.Min
	}
	if p != nil {
		return *p
	}
	v, _ := analysis.DefaultThreshold(c.Type)
	return v
}

func set(c *mgv1.CheckSpec, v float64) {
	if analysis.HigherIsStricter(c.Type) {
		c.Min = &v
	} else {
		c.Max = &v
	}
}

func looser(checkType string, rollout, policy float64) bool {
	if analysis.HigherIsStricter(checkType) {
		return rollout < policy
	}
	return rollout > policy
}

func field(checkType string) string {
	if analysis.HigherIsStricter(checkType) {
		return "min"
	}
	return "max"
}

func fmtf(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
