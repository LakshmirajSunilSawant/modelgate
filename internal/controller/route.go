package controller

import (
	"math"

	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

// trafficPlan is what the HTTPRoute should do for the current status.
type trafficPlan struct {
	canaryWeight  int32 // share of live traffic served by the canary
	mirrorPercent int32 // share of traffic mirrored to the canary; 0 for none
}

// planFor derives the traffic plan from status alone, so it is idempotent
// and survives controller restarts.
func planFor(mr *mgv1.ModelRollout) trafficPlan {
	st := mr.Status
	if st.CanaryRevision == "" {
		return trafficPlan{}
	}
	if st.CurrentStep == mgv1.ShadowStepIndex {
		rate := 1.0
		if sh := mr.Spec.Strategy.Shadow; sh != nil && sh.SampleRate > 0 {
			rate = sh.SampleRate
		}
		return trafficPlan{mirrorPercent: int32(max(1, math.Round(rate*100)))}
	}
	steps := mr.Spec.Strategy.Steps
	if int(st.CurrentStep) < len(steps) {
		return trafficPlan{canaryWeight: steps[st.CurrentStep].Weight}
	}
	return trafficPlan{}
}

// applyPlan rewrites the backends and mirror filters of every rule in the
// route. It returns true if the route changed. Filters that do not mirror
// to the canary service are kept as they are.
func applyPlan(route *gwv1.HTTPRoute, mr *mgv1.ModelRollout, plan trafficPlan) bool {
	stableSvc, canarySvc := stableServiceName(mr), canaryServiceName(mr)
	port := gwv1.PortNumber(servicePort)
	backends := []gwv1.HTTPBackendRef{{BackendRef: gwv1.BackendRef{
		BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(stableSvc), Port: &port},
		Weight:                 ptr(100 - plan.canaryWeight),
	}}}
	if plan.canaryWeight > 0 {
		backends = append(backends, gwv1.HTTPBackendRef{BackendRef: gwv1.BackendRef{
			BackendObjectReference: gwv1.BackendObjectReference{Name: gwv1.ObjectName(canarySvc), Port: &port},
			Weight:                 ptr(plan.canaryWeight),
		}})
	}

	changed := false
	if len(route.Spec.Rules) == 0 {
		route.Spec.Rules = []gwv1.HTTPRouteRule{{}}
		changed = true
	}
	for i := range route.Spec.Rules {
		rule := &route.Spec.Rules[i]
		var filters []gwv1.HTTPRouteFilter
		for _, f := range rule.Filters {
			if f.Type == gwv1.HTTPRouteFilterRequestMirror && f.RequestMirror != nil &&
				string(f.RequestMirror.BackendRef.Name) == canarySvc {
				continue
			}
			filters = append(filters, f)
		}
		if plan.mirrorPercent > 0 {
			filters = append(filters, gwv1.HTTPRouteFilter{
				Type: gwv1.HTTPRouteFilterRequestMirror,
				RequestMirror: &gwv1.HTTPRequestMirrorFilter{
					BackendRef: gwv1.BackendObjectReference{Name: gwv1.ObjectName(canarySvc), Port: &port},
					Percent:    ptr(plan.mirrorPercent),
				},
			})
		}
		if !backendsEqual(rule.BackendRefs, backends) || !filtersEqual(rule.Filters, filters) {
			rule.BackendRefs = append([]gwv1.HTTPBackendRef(nil), backends...)
			rule.Filters = filters
			changed = true
		}
	}
	return changed
}

func backendsEqual(a, b []gwv1.HTTPBackendRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || derefOr(a[i].Weight, 1) != derefOr(b[i].Weight, 1) ||
			derefOr(a[i].Port, 0) != derefOr(b[i].Port, 0) || len(a[i].Filters) > 0 {
			return false
		}
	}
	return true
}

func filtersEqual(a, b []gwv1.HTTPRouteFilter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type {
			return false
		}
		am, bm := a[i].RequestMirror, b[i].RequestMirror
		if (am == nil) != (bm == nil) {
			return false
		}
		if am != nil && (am.BackendRef.Name != bm.BackendRef.Name || derefOr(am.Percent, 100) != derefOr(bm.Percent, 100)) {
			return false
		}
	}
	return true
}

func derefOr[T any](p *T, def T) T {
	if p == nil {
		return def
	}
	return *p
}
