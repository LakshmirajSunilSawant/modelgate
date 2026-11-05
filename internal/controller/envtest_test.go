package controller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"
	"sigs.k8s.io/yaml"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/analysis"
)

// These tests run against a real kube-apiserver and etcd from envtest, so
// the generated CRD schema, CEL rules, status subresource, and resource
// versions are all enforced. They skip unless KUBEBUILDER_ASSETS is set
// (see `make envtest`).

func startEnvtest(t *testing.T) client.Client {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make envtest`")
	}
	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	te.CRDInstallOptions.Paths = append(te.CRDInstallOptions.Paths, filepath.Join("..", "..", "config", "crd", "external"))
	cfg, err := te.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() { _ = te.Stop() })

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = mgv1.AddToScheme(scheme)
	_ = gwv1.Install(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// sampleRollout loads the ModelRollout from config/samples/demo.yaml.
func sampleRollout(t *testing.T) (*mgv1.ModelRollout, *gwv1.HTTPRoute) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "demo.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var mr *mgv1.ModelRollout
	var route *gwv1.HTTPRoute
	for _, doc := range strings.Split(string(raw), "\n---") {
		switch {
		case strings.Contains(doc, "kind: ModelRollout"):
			mr = &mgv1.ModelRollout{}
			if err := yaml.UnmarshalStrict([]byte(doc), mr); err != nil {
				t.Fatalf("sample ModelRollout: %v", err)
			}
		case strings.Contains(doc, "kind: HTTPRoute"):
			route = &gwv1.HTTPRoute{}
			if err := yaml.UnmarshalStrict([]byte(doc), route); err != nil {
				t.Fatalf("sample HTTPRoute: %v", err)
			}
		}
	}
	if mr == nil || route == nil {
		t.Fatal("sample missing ModelRollout or HTTPRoute")
	}
	return mr, route
}

func createNamespace(t *testing.T, c client.Client, name string) {
	t.Helper()
	if err := c.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}); err != nil {
		t.Fatal(err)
	}
}

func TestEnvtestCRDValidation(t *testing.T) {
	c := startEnvtest(t)
	ctx := context.Background()
	createNamespace(t, c, "demo")

	mr, _ := sampleRollout(t)
	mr.Spec.Template.Annotations = map[string]string{"prometheus.io/scrape": "true"}
	mr.Spec.Template.Labels = map[string]string{"team": "fraud"}
	if err := c.Create(ctx, mr.DeepCopy()); err != nil {
		t.Fatalf("sample rejected by CRD: %v", err)
	}
	got := &mgv1.ModelRollout{}
	_ = c.Get(ctx, client.ObjectKeyFromObject(mr), got)
	if got.Spec.Template.Annotations["prometheus.io/scrape"] != "true" || got.Spec.Template.Labels["team"] != "fraud" {
		t.Errorf("pod template metadata pruned by CRD schema: %+v", got.Spec.Template.ObjectMeta)
	}
	if got.Spec.Analysis.Alpha != 0.01 || got.Spec.Capture.SampleRate != 1 {
		t.Errorf("CRD defaults not applied: alpha=%v sampleRate=%v", got.Spec.Analysis.Alpha, got.Spec.Capture.SampleRate)
	}

	cases := map[string]func(*mgv1.ModelRollout){
		"first step above 50%": func(m *mgv1.ModelRollout) { m.Spec.Strategy.Steps[0].Weight = 60 },
		"no shadow or steps":   func(m *mgv1.ModelRollout) { m.Spec.Strategy = mgv1.StrategySpec{} },
		"unknown check type":   func(m *mgv1.ModelRollout) { m.Spec.Analysis.Checks[0].Type = "vibes" },
		"no checks":            func(m *mgv1.ModelRollout) { m.Spec.Analysis.Checks = nil },
		"sample rate above 1":  func(m *mgv1.ModelRollout) { m.Spec.Strategy.Shadow.SampleRate = 1.5 },
		"bad output type":      func(m *mgv1.ModelRollout) { m.Spec.Output.Type = "text" },
	}
	for name, mutate := range cases {
		bad := mr.DeepCopy()
		bad.Name = "bad-" + strings.ReplaceAll(strings.ReplaceAll(name, " ", "-"), "%", "")
		mutate(bad)
		if err := c.Create(ctx, bad); err == nil {
			t.Errorf("%s: accepted, want rejection", name)
		}
	}
}

// TestEnvtestRolloutLifecycle drives the reconciler against a real API
// server: first deploy, shadow, steps, promote, then a bad canary rolled back.
func TestEnvtestRolloutLifecycle(t *testing.T) {
	c := startEnvtest(t)
	ctx := context.Background()
	createNamespace(t, c, "demo")
	mr, route := sampleRollout(t)
	if err := c.Create(ctx, route); err != nil {
		t.Fatalf("sample HTTPRoute rejected: %v", err)
	}
	if err := c.Create(ctx, mr); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKeyFromObject(mr)

	clock := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	an := &fakeAnalyzer{verdict: analysis.Verdict{Decision: analysis.Pass}}
	r := &ModelRolloutReconciler{
		Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(1000), Analyzer: an,
		Sidecar:    SidecarConfig{Image: "modelgate-sidecar:dev", AnalyzerURL: "http://analyzer/v1/events"},
		KillSwitch: types.NamespacedName{Namespace: "modelgate-system", Name: "modelgate-config"},
		Now:        func() time.Time { return clock },
	}
	reconcile := func() {
		t.Helper()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}
	get := func() *mgv1.ModelRollout {
		t.Helper()
		m := &mgv1.ModelRollout{}
		if err := c.Get(ctx, key, m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	// envtest runs no kube-controller-manager, so mark Deployments ready by hand.
	markAvailable := func() {
		t.Helper()
		var list appsv1.DeploymentList
		_ = c.List(ctx, &list, client.InNamespace("demo"))
		for i := range list.Items {
			d := &list.Items[i]
			n := *d.Spec.Replicas
			d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, Replicas: n, ReadyReplicas: n, AvailableReplicas: n, UpdatedReplicas: n}
			if err := c.Status().Update(ctx, d); err != nil {
				t.Fatal(err)
			}
		}
	}
	routeState := func() (stable, canary, mirror int32) {
		t.Helper()
		rt := &gwv1.HTTPRoute{}
		_ = c.Get(ctx, types.NamespacedName{Namespace: "demo", Name: "demo-model"}, rt)
		for _, b := range rt.Spec.Rules[0].BackendRefs {
			if strings.HasSuffix(string(b.Name), "-stable") {
				stable = *b.Weight
			} else {
				canary = *b.Weight
			}
		}
		for _, f := range rt.Spec.Rules[0].Filters {
			if f.RequestMirror != nil && f.RequestMirror.Percent != nil {
				mirror = *f.RequestMirror.Percent
			}
		}
		if len(rt.Spec.Rules[0].Matches) == 0 {
			t.Error("route matches were dropped")
		}
		return
	}
	setVariant := func(v string) {
		t.Helper()
		m := get()
		m.Spec.Template.Spec.Containers[0].Env[0].Value = v
		if err := c.Update(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	reconcile()
	v1 := get().Status.StableRevision
	if s, cw, _ := routeState(); v1 == "" || s != 100 || cw != 0 {
		t.Fatalf("first deploy: rev=%q weights %d/%d", v1, s, cw)
	}
	markAvailable()

	setVariant("identical")
	reconcile()
	markAvailable()
	reconcile()
	m := get()
	if m.Status.CurrentStep != mgv1.ShadowStepIndex || m.Status.Phase != mgv1.PhaseProgressing {
		t.Fatalf("not in shadow: %+v", m.Status)
	}
	if s, cw, mir := routeState(); s != 100 || cw != 0 || mir != 100 {
		t.Errorf("shadow route %d/%d mirror %d (experimental percent field must persist)", s, cw, mir)
	}
	if m.Status.LastVerdict == nil || m.Status.ObservedGeneration != m.Generation {
		t.Errorf("status not written through subresource: %+v", m.Status)
	}

	for i, want := range []int32{5, 25, 50} {
		clock = clock.Add(6 * time.Minute)
		reconcile()
		if st := get().Status; st.CanaryWeight != want || st.CurrentStep != int32(i) {
			t.Fatalf("step %d: %+v", i, st)
		}
		if s, cw, mir := routeState(); s != 100-want || cw != want || mir != 0 {
			t.Errorf("step %d route %d/%d mirror %d", i, s, cw, mir)
		}
	}
	clock = clock.Add(6 * time.Minute)
	reconcile()
	m = get()
	v2 := m.Status.StableRevision
	if m.Status.Phase != mgv1.PhaseHealthy || v2 == v1 || m.Status.CanaryRevision != "" {
		t.Fatalf("not promoted: %+v", m.Status)
	}
	if cond := findCondition(m, mgv1.ConditionReady); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition after promote: %+v", cond)
	}

	// Bad canary: hard failure in shadow rolls back.
	setVariant("nan")
	reconcile()
	markAvailable()
	an.verdict = analysis.Verdict{Decision: analysis.Fail, HardFailure: true, Reason: "valid: 110 of 1200 canary outputs invalid"}
	reconcile()
	m = get()
	if m.Status.Phase != mgv1.PhaseDegraded || m.Status.StableRevision != v2 || m.Status.AbortedRevision == "" {
		t.Fatalf("not rolled back: %+v", m.Status)
	}
	if cond := findCondition(m, mgv1.ConditionStalled); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Stalled condition after rollback: %+v", cond)
	}
	if s, cw, mir := routeState(); s != 100 || cw != 0 || mir != 0 {
		t.Errorf("post-rollback route %d/%d mirror %d", s, cw, mir)
	}
	var deps appsv1.DeploymentList
	_ = c.List(ctx, &deps, client.InNamespace("demo"))
	if len(deps.Items) != 1 || deps.Items[0].Labels[labelRevision] != v2 {
		t.Errorf("want only stable deployment %s, got %d", v2, len(deps.Items))
	}
}

func findCondition(m *mgv1.ModelRollout, t string) *metav1.Condition {
	for i := range m.Status.Conditions {
		if m.Status.Conditions[i].Type == t {
			return &m.Status.Conditions[i]
		}
	}
	return nil
}
