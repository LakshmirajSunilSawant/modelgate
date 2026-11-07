package controller

import (
	"context"
	"errors"
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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/analysis"
)

type fakeAnalyzer struct {
	verdict analysis.Verdict
	err     error
	calls   []analysis.Request
}

func (f *fakeAnalyzer) Verdict(_ context.Context, req analysis.Request) (analysis.Verdict, error) {
	f.calls = append(f.calls, req)
	return f.verdict, f.err
}

type env struct {
	t     *testing.T
	c     client.Client
	r     *ModelRolloutReconciler
	an    *fakeAnalyzer
	clock time.Time
	key   types.NamespacedName
}

func newEnv(t *testing.T) *env {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = mgv1.AddToScheme(scheme)
	_ = gwv1.Install(scheme)

	mr := &mgv1.ModelRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "fraud", Namespace: "ml", UID: "uid-1"},
		Spec: mgv1.ModelRolloutSpec{
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "model", Image: "fraud:v1"}}}},
			RouteRef: mgv1.RouteRef{Name: "fraud"},
			Output:   mgv1.OutputSpec{Type: mgv1.OutputClassification, LabelPath: "$.label"},
			Strategy: mgv1.StrategySpec{
				Shadow: &mgv1.ShadowSpec{Duration: metav1.Duration{Duration: 10 * time.Minute}, SampleRate: 0.2},
				Steps: []mgv1.Step{
					{Weight: 5, Pause: metav1.Duration{Duration: 10 * time.Minute}},
					{Weight: 50, Pause: metav1.Duration{Duration: 10 * time.Minute}},
				},
			},
			Analysis: mgv1.AnalysisSpec{MinSamples: 100, FailureLimit: 2, Checks: []mgv1.CheckSpec{{Name: "agree", Type: "agreementRate"}}},
		},
	}
	route := &gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{Name: "fraud", Namespace: "ml"}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(mr, route).
		WithStatusSubresource(&mgv1.ModelRollout{}).
		Build()
	e := &env{t: t, c: c, an: &fakeAnalyzer{verdict: analysis.Verdict{Decision: analysis.Pass}},
		clock: time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC), key: types.NamespacedName{Namespace: "ml", Name: "fraud"}}
	e.r = &ModelRolloutReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(100), Analyzer: e.an,
		Sidecar:    SidecarConfig{Image: "ghcr.io/modelgate/sidecar:dev", AnalyzerURL: "http://analyzer"},
		KillSwitch: types.NamespacedName{Namespace: "modelgate-system", Name: "modelgate-config"},
		Now:        func() time.Time { return e.clock },
	}
	return e
}

func (e *env) reconcile() ctrl.Result {
	e.t.Helper()
	res, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: e.key})
	if err != nil {
		e.t.Fatalf("reconcile: %v", err)
	}
	return res
}

func (e *env) get() *mgv1.ModelRollout {
	e.t.Helper()
	mr := &mgv1.ModelRollout{}
	if err := e.c.Get(context.Background(), e.key, mr); err != nil {
		e.t.Fatal(err)
	}
	return mr
}

func (e *env) update(fn func(*mgv1.ModelRollout)) {
	e.t.Helper()
	mr := e.get()
	fn(mr)
	if err := e.c.Update(context.Background(), mr); err != nil {
		e.t.Fatal(err)
	}
}

// markAvailable simulates the Deployment controller.
func (e *env) markAvailable() {
	e.t.Helper()
	var list appsv1.DeploymentList
	_ = e.c.List(context.Background(), &list)
	for i := range list.Items {
		d := &list.Items[i]
		d.Status = appsv1.DeploymentStatus{ObservedGeneration: d.Generation, AvailableReplicas: *d.Spec.Replicas, UpdatedReplicas: *d.Spec.Replicas}
		if err := e.c.Status().Update(context.Background(), d); err != nil {
			e.t.Fatal(err)
		}
	}
}

func (e *env) deployments() map[string]bool {
	var list appsv1.DeploymentList
	_ = e.c.List(context.Background(), &list)
	out := map[string]bool{}
	for _, d := range list.Items {
		out[d.Labels[labelRevision]] = true
	}
	return out
}

// weights returns stable weight, canary weight, and mirror percent of rule 0.
func (e *env) weights() (stable, canary, mirror int32) {
	route := &gwv1.HTTPRoute{}
	_ = e.c.Get(context.Background(), e.key, route)
	rule := route.Spec.Rules[0]
	for _, b := range rule.BackendRefs {
		switch string(b.Name) {
		case "fraud-stable":
			stable = *b.Weight
		case "fraud-canary":
			canary = *b.Weight
		}
	}
	for _, f := range rule.Filters {
		if f.RequestMirror != nil {
			mirror = *f.RequestMirror.Percent
		}
	}
	return
}

func (e *env) serviceRevision(name string) string {
	svc := &corev1.Service{}
	_ = e.c.Get(context.Background(), types.NamespacedName{Namespace: "ml", Name: name}, svc)
	return svc.Spec.Selector[labelRevision]
}

func (e *env) startRollout() (oldRev, newRev string) {
	e.t.Helper()
	e.reconcile()
	oldRev = e.get().Status.StableRevision
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Template.Spec.Containers[0].Image = "fraud:v2" })
	e.reconcile()
	e.markAvailable()
	newRev = e.get().Status.CanaryRevision
	if newRev == "" || newRev == oldRev {
		e.t.Fatalf("rollout did not start: %+v", e.get().Status)
	}
	return oldRev, newRev
}

func TestFirstDeploy(t *testing.T) {
	e := newEnv(t)
	e.reconcile()
	st := e.get().Status
	if st.Phase != mgv1.PhaseHealthy || st.StableRevision == "" || st.CanaryRevision != "" {
		t.Fatalf("status %+v", st)
	}
	if !e.deployments()[st.StableRevision] {
		t.Error("stable deployment missing")
	}
	if s, c, m := e.weights(); s != 100 || c != 0 || m != 0 {
		t.Errorf("weights %d/%d mirror %d", s, c, m)
	}
	// Sidecar injected and service targets it.
	var d appsv1.Deployment
	_ = e.c.Get(context.Background(), types.NamespacedName{Namespace: "ml", Name: "fraud-" + st.StableRevision}, &d)
	if n := len(d.Spec.Template.Spec.Containers); n != 2 || d.Spec.Template.Spec.Containers[1].Name != sidecarName {
		t.Errorf("sidecar not injected: %d containers", n)
	}
}

func TestHappyPathPromotes(t *testing.T) {
	e := newEnv(t)
	oldRev, newRev := e.startRollout()

	e.reconcile()
	st := e.get().Status
	if st.CurrentStep != mgv1.ShadowStepIndex || st.Phase != mgv1.PhaseProgressing {
		t.Fatalf("not in shadow: %+v", st)
	}
	if s, c, m := e.weights(); s != 100 || c != 0 || m != 20 {
		t.Errorf("shadow weights %d/%d mirror %d", s, c, m)
	}
	if e.an.calls[len(e.an.calls)-1].Phase != analysis.PhaseShadow {
		t.Error("analyzer not asked for shadow phase")
	}

	e.clock = e.clock.Add(11 * time.Minute)
	e.reconcile()
	if st := e.get().Status; st.CurrentStep != 0 || st.CanaryWeight != 5 {
		t.Fatalf("did not advance to step 1: %+v", st)
	}
	if s, c, m := e.weights(); s != 95 || c != 5 || m != 0 {
		t.Errorf("step 1 weights %d/%d mirror %d", s, c, m)
	}

	e.clock = e.clock.Add(11 * time.Minute)
	e.reconcile()
	e.clock = e.clock.Add(11 * time.Minute)
	e.reconcile()
	st = e.get().Status
	if st.Phase != mgv1.PhaseHealthy || st.StableRevision != newRev || st.CanaryRevision != "" {
		t.Fatalf("not promoted: %+v", st)
	}
	if s, c, _ := e.weights(); s != 100 || c != 0 {
		t.Errorf("post-promote weights %d/%d", s, c)
	}
	if e.serviceRevision("fraud-stable") != newRev {
		t.Error("stable service not switched")
	}
	if d := e.deployments(); d[oldRev] || !d[newRev] {
		t.Errorf("deployments after promote: %v", d)
	}
}

func TestHardFailureRollsBackImmediately(t *testing.T) {
	e := newEnv(t)
	oldRev, newRev := e.startRollout()
	e.an.verdict = analysis.Verdict{Decision: analysis.Fail, HardFailure: true, Reason: "valid: 40 of 2000 outputs invalid"}
	e.reconcile()
	st := e.get().Status
	if st.Phase != mgv1.PhaseDegraded || st.AbortedRevision != newRev || st.StableRevision != oldRev || st.CanaryRevision != "" {
		t.Fatalf("not rolled back: %+v", st)
	}
	if s, c, m := e.weights(); s != 100 || c != 0 || m != 0 {
		t.Errorf("weights %d/%d mirror %d", s, c, m)
	}
	if d := e.deployments(); d[newRev] {
		t.Error("canary deployment not removed")
	}
	// Not retried while the template is unchanged.
	e.reconcile()
	if e.get().Status.CanaryRevision != "" {
		t.Error("aborted revision retried")
	}
}

func TestSoftFailureNeedsFailureLimit(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	e.an.verdict = analysis.Verdict{Decision: analysis.Fail, Reason: "psi too high"}
	e.reconcile()
	if st := e.get().Status; st.CanaryRevision == "" || st.ConsecutiveFailures != 1 {
		t.Fatalf("rolled back on first soft failure: %+v", st)
	}
	e.clock = e.clock.Add(31 * time.Second)
	e.reconcile()
	if st := e.get().Status; st.Phase != mgv1.PhaseDegraded {
		t.Fatalf("not rolled back after limit: %+v", st)
	}
}

// Extra reconciles from watch events must not count as new evaluations.
func TestLooksAreSpacedByInterval(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	e.an.verdict = analysis.Verdict{Decision: analysis.Fail, Reason: "psi too high"}
	e.reconcile()
	calls := len(e.an.calls)
	e.reconcile()
	e.reconcile()
	if st := e.get().Status; st.ConsecutiveFailures != 1 || st.CanaryRevision == "" {
		t.Fatalf("back-to-back reconciles counted as failures: %+v", st)
	}
	if len(e.an.calls) != calls {
		t.Errorf("analyzer called %d extra times within the interval", len(e.an.calls)-calls)
	}
	e.clock = e.clock.Add(31 * time.Second)
	e.reconcile()
	if st := e.get().Status; st.Phase != mgv1.PhaseDegraded {
		t.Fatalf("second spaced failure did not roll back: %+v", st)
	}
}

// Capture settings live outside the pod template; changing them must reach
// both the canary and the older stable Deployment.
func TestCaptureChangeReachesBothDeployments(t *testing.T) {
	e := newEnv(t)
	oldRev, newRev := e.startRollout()
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Capture.SampleRate = 0.5 })
	e.reconcile()
	for _, rev := range []string{oldRev, newRev} {
		var d appsv1.Deployment
		_ = e.c.Get(context.Background(), types.NamespacedName{Namespace: "ml", Name: "fraud-" + rev}, &d)
		got := ""
		for _, c := range d.Spec.Template.Spec.Containers {
			for _, env := range c.Env {
				if env.Name == "MODELGATE_SAMPLE_RATE" {
					got = env.Value
				}
			}
		}
		if got != "0.5" {
			t.Errorf("deployment %s sample rate %q, want 0.5", rev, got)
		}
		if n := len(d.Spec.Template.Spec.Containers); n != 2 {
			t.Errorf("deployment %s has %d containers", rev, n)
		}
	}
}

func TestNamespacePolicyIsEnforced(t *testing.T) {
	e := newEnv(t)
	loose, strict := 0.5, 0.01
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.Analysis.Checks = append(mr.Spec.Analysis.Checks, mgv1.CheckSpec{Name: "errors", Type: "errorRate", Max: &loose})
	})
	pol := &mgv1.ModelGatePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: mgv1.PolicyName},
		Spec: mgv1.ModelGatePolicySpec{
			Checks:             []mgv1.CheckSpec{{Name: "errors", Type: "errorRate", Max: &strict}, {Name: "valid", Type: "validity"}},
			MaxFirstStepWeight: ptr(int32(2)),
		},
	}
	if err := e.c.Create(context.Background(), pol); err != nil {
		t.Fatal(err)
	}
	e.startRollout()
	e.reconcile()
	req := e.an.calls[len(e.an.calls)-1]
	got := map[string]analysis.CheckSpec{}
	for _, c := range req.Checks {
		got[c.Name] = c
	}
	if c := got["errors"]; c.Max == nil || *c.Max != strict {
		t.Errorf("errorRate threshold sent to analyzer: %+v", c)
	}
	if _, ok := got["policy-valid"]; !ok {
		t.Errorf("required check missing from analysis: %+v", req.Checks)
	}
	mr := e.get()
	if c := findCondition(mr, ConditionPolicyApplied); c == nil || c.Reason != "Tightened" {
		t.Errorf("PolicyApplied condition: %+v", c)
	}
	if v := *mr.Spec.Analysis.Checks[len(mr.Spec.Analysis.Checks)-1].Max; v != loose {
		t.Errorf("policy was written back into the stored spec (max %v)", v)
	}
	e.clock = e.clock.Add(11 * time.Minute)
	e.reconcile()
	if _, c, _ := e.weights(); c != 2 {
		t.Errorf("first step weight %d, want policy cap 2", c)
	}
}

func TestAnalyzerDownHoldsAndNeverPromotes(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	e.an.err = errors.New("connection refused")
	for i := 0; i < 5; i++ {
		e.clock = e.clock.Add(time.Hour)
		e.reconcile()
	}
	st := e.get().Status
	if st.Phase != mgv1.PhasePaused || st.CurrentStep != mgv1.ShadowStepIndex || st.CanaryRevision == "" {
		t.Fatalf("expected hold in shadow: %+v", st)
	}
	if s, c, _ := e.weights(); s != 100 || c != 0 {
		t.Errorf("weights moved: %d/%d", s, c)
	}
}

func TestInconclusiveHolds(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	e.an.verdict = analysis.Verdict{Decision: analysis.Inconclusive, Reason: "agree: 50/100 samples"}
	e.clock = e.clock.Add(time.Hour)
	e.reconcile()
	if st := e.get().Status; st.CurrentStep != mgv1.ShadowStepIndex || st.Phase != mgv1.PhaseProgressing {
		t.Fatalf("advanced without evidence: %+v", st)
	}
}

func TestKillSwitchFreezes(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "modelgate-system", Name: "modelgate-config"}, Data: map[string]string{KillSwitchKey: "true"}}
	if err := e.c.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	calls := len(e.an.calls)
	e.clock = e.clock.Add(time.Hour)
	e.reconcile()
	if st := e.get().Status; st.Phase != mgv1.PhasePaused || st.CurrentStep != mgv1.ShadowStepIndex {
		t.Fatalf("not frozen: %+v", st)
	}
	if len(e.an.calls) != calls {
		t.Error("analysis ran while frozen")
	}
}

func TestManualAbortAndPromote(t *testing.T) {
	e := newEnv(t)
	_, newRev := e.startRollout()
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Annotations = map[string]string{mgv1.ActionAnnotation: "promote", mgv1.ActionByAnnotation: "alice@example.com"}
	})
	e.reconcile()
	mr := e.get()
	if mr.Status.StableRevision != newRev || mr.Status.Phase != mgv1.PhaseHealthy {
		t.Fatalf("not force-promoted: %+v", mr.Status)
	}
	if _, ok := mr.Annotations[mgv1.ActionAnnotation]; ok {
		t.Error("action annotation not cleared")
	}

	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Template.Spec.Containers[0].Image = "fraud:v3" })
	e.reconcile()
	e.markAvailable()
	e.update(func(mr *mgv1.ModelRollout) { mr.Annotations[mgv1.ActionAnnotation] = "abort" })
	e.reconcile()
	if st := e.get().Status; st.Phase != mgv1.PhaseDegraded || st.StableRevision != newRev {
		t.Fatalf("not aborted: %+v", st)
	}
}

func TestRevertTemplateCancelsCanary(t *testing.T) {
	e := newEnv(t)
	oldRev, newRev := e.startRollout()
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Template.Spec.Containers[0].Image = "fraud:v1" })
	e.reconcile()
	st := e.get().Status
	if st.StableRevision != oldRev || st.CanaryRevision != "" || st.Phase != mgv1.PhaseHealthy {
		t.Fatalf("status %+v", st)
	}
	if e.deployments()[newRev] {
		t.Error("canary deployment kept")
	}
}

func TestResumeAfterCrashIsIdempotent(t *testing.T) {
	e := newEnv(t)
	e.startRollout()
	e.clock = e.clock.Add(11 * time.Minute)
	e.reconcile()
	before := e.get()
	// A fresh reconciler (controller restart) with the same status must not
	// move traffic or change the stage.
	e.r = &ModelRolloutReconciler{Client: e.c, Scheme: e.r.Scheme, Recorder: record.NewFakeRecorder(100), Analyzer: e.an, Sidecar: e.r.Sidecar, Now: e.r.Now}
	e.an.verdict = analysis.Verdict{Decision: analysis.Inconclusive}
	e.reconcile()
	after := e.get()
	if after.Status.CurrentStep != before.Status.CurrentStep || after.Status.CanaryRevision != before.Status.CanaryRevision {
		t.Fatalf("state changed across restart: %+v -> %+v", before.Status, after.Status)
	}
	if s, c, _ := e.weights(); s != 95 || c != 5 {
		t.Errorf("weights %d/%d", s, c)
	}
}
