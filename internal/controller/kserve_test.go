package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

func TestGRPCRolloutUsesGRPCRouteAndH2C(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	other := gwv1.GRPCRouteFilter{Type: gwv1.GRPCRouteFilterRequestHeaderModifier,
		RequestHeaderModifier: &gwv1.HTTPHeaderFilter{Set: []gwv1.HTTPHeader{{Name: "x-team", Value: "fraud"}}}}
	route := &gwv1.GRPCRoute{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "fraud-grpc"},
		Spec:       gwv1.GRPCRouteSpec{Rules: []gwv1.GRPCRouteRule{{Filters: []gwv1.GRPCRouteFilter{other}}}},
	}
	if err := e.c.Create(ctx, route); err != nil {
		t.Fatal(err)
	}
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.RouteRef = mgv1.RouteRef{Name: "fraud-grpc", Kind: mgv1.RouteKindGRPC}
		mr.Spec.Output = mgv1.OutputSpec{Type: mgv1.OutputClassification, Protocol: mgv1.ProtocolKServeV2GRPC,
			LabelTensor: &mgv1.TensorRef{Name: "probs", Reduce: mgv1.ReduceArgmax},
			ScoreTensor: &mgv1.TensorRef{Name: "probs", Reduce: mgv1.ReduceMax}}
	})
	_, newRev := e.startRollout()

	grpcRoute := func() gwv1.GRPCRouteRule {
		r := &gwv1.GRPCRoute{}
		if err := e.c.Get(ctx, types.NamespacedName{Namespace: "ml", Name: "fraud-grpc"}, r); err != nil {
			t.Fatal(err)
		}
		return r.Spec.Rules[0]
	}
	// Shadow: all traffic to stable, mirrored at 20% to the canary. The
	// canary is listed at weight 0 so Envoy Gateway resolves the mirror.
	rule := grpcRoute()
	if len(rule.BackendRefs) != 2 || rule.BackendRefs[0].Name != "fraud-stable" || *rule.BackendRefs[0].Weight != 100 ||
		rule.BackendRefs[1].Name != "fraud-canary" || *rule.BackendRefs[1].Weight != 0 {
		t.Errorf("shadow backends %+v", rule.BackendRefs)
	}
	if len(rule.Filters) != 2 || rule.Filters[0].Type != gwv1.GRPCRouteFilterRequestHeaderModifier ||
		rule.Filters[1].Type != gwv1.GRPCRouteFilterRequestMirror || *rule.Filters[1].RequestMirror.Percent != 20 ||
		rule.Filters[1].RequestMirror.BackendRef.Name != "fraud-canary" {
		t.Errorf("shadow filters %+v", rule.Filters)
	}

	// First step: 5% of calls to the canary, mirror gone, other filter kept.
	e.clock = e.clock.Add(11 * 60e9)
	e.reconcile()
	e.reconcile()
	rule = grpcRoute()
	if len(rule.BackendRefs) != 2 || *rule.BackendRefs[0].Weight != 95 || *rule.BackendRefs[1].Weight != 5 {
		t.Errorf("step backends %+v", rule.BackendRefs)
	}
	if len(rule.Filters) != 1 || rule.Filters[0].Type != gwv1.GRPCRouteFilterRequestHeaderModifier {
		t.Errorf("step filters %+v", rule.Filters)
	}

	svc := &corev1.Service{}
	if err := e.c.Get(ctx, types.NamespacedName{Namespace: "ml", Name: "fraud-canary"}, svc); err != nil {
		t.Fatal(err)
	}
	if p := svc.Spec.Ports[0]; p.AppProtocol == nil || *p.AppProtocol != "kubernetes.io/h2c" || p.Name != "grpc" {
		t.Errorf("service port %+v", p)
	}
	env := e.sidecarEnv(newRev)
	if env["MODELGATE_PROTOCOL"] != "kserveV2gRPC" || env["MODELGATE_LABEL_TENSOR"] != `{"name":"probs","reduce":"argmax"}` ||
		env["MODELGATE_SCORE_TENSOR"] != `{"name":"probs","reduce":"max"}` {
		t.Errorf("sidecar env %v", env)
	}
}

func TestJSONRolloutSidecarUnchanged(t *testing.T) {
	mr := &mgv1.ModelRollout{ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "fraud"}}
	mr.Spec.Output = mgv1.OutputSpec{Type: mgv1.OutputClassification, LabelPath: "$.label"}
	before := hashJSON(buildSidecar(mr, "r1", SidecarConfig{}))
	mr.Spec.Output.Protocol = mgv1.ProtocolJSON
	if after := hashJSON(buildSidecar(mr, "r1", SidecarConfig{})); after != before {
		t.Error("an explicit json protocol changed the sidecar, which would restart pods on upgrade")
	}
}
