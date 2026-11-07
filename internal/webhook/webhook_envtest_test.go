package webhook

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

// TestWebhookEnforcesPolicy runs the webhook behind a real kube-apiserver.
// It skips unless KUBEBUILDER_ASSETS is set (see `make envtest`).
func TestWebhookEnforcesPolicy(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run `make envtest`")
	}
	te := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{Paths: []string{filepath.Join("..", "..", "config", "webhook")}},
	}
	cfg, err := te.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() { _ = te.Stop() })

	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = mgv1.AddToScheme(scheme)
	wo := te.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host: wo.LocalServingHost, Port: wo.LocalServingPort, CertDir: wo.LocalServingCertDir,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := Setup(mgr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = mgr.Start(ctx) }()
	waitForServer(t, fmt.Sprintf("%s:%d", wo.LocalServingHost, wo.LocalServingPort))

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}})

	strict, loose := 0.01, 0.2
	pol := &mgv1.ModelGatePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: mgv1.PolicyName},
		Spec: mgv1.ModelGatePolicySpec{
			Checks:             []mgv1.CheckSpec{{Name: "errors", Type: "errorRate", Max: &strict}, {Name: "valid", Type: "validity"}},
			MaxFirstStepWeight: ptr(int32(10)),
		},
	}
	if err := c.Create(ctx, pol); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &mgv1.ModelGatePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "other"}}); err == nil {
		t.Error("second policy with a non-default name was accepted")
	}

	rollout := func(name string, errMax float64, firstStep int32) *mgv1.ModelRollout {
		return &mgv1.ModelRollout{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: name},
			Spec: mgv1.ModelRolloutSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "m", Image: "m:1"}}}},
				RouteRef: mgv1.RouteRef{Name: "r"},
				Output:   mgv1.OutputSpec{Type: mgv1.OutputClassification, LabelPath: "$.label"},
				Strategy: mgv1.StrategySpec{Steps: []mgv1.Step{{Weight: firstStep, Pause: metav1.Duration{Duration: time.Minute}}}},
				Analysis: mgv1.AnalysisSpec{Checks: []mgv1.CheckSpec{{Name: "errors", Type: "errorRate", Max: &errMax}}},
			},
		}
	}

	err = c.Create(ctx, rollout("loose", loose, 25))
	if err == nil {
		t.Fatal("rollout looser than policy was accepted")
	}
	for _, want := range []string{"errorRate", "0.2", "first step weight 25%"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("rejection message %q does not mention %q", err.Error(), want)
		}
	}

	ok := rollout("ok", 0.005, 5)
	if err := c.Create(ctx, ok); err != nil {
		t.Fatalf("compliant rollout rejected: %v", err)
	}

	// Tighten the policy past the existing rollout: metadata-only updates
	// (manual-action annotations) must still pass, spec changes must not.
	pol.Spec.Checks[0].Max = ptr(0.001)
	if err := c.Update(ctx, pol); err != nil {
		t.Fatal(err)
	}
	ok.Annotations = map[string]string{mgv1.ActionAnnotation: "abort"}
	if err := c.Update(ctx, ok); err != nil {
		t.Errorf("annotation-only update rejected: %v", err)
	}
	ok.Spec.Template.Spec.Containers[0].Image = "m:2"
	if err := c.Update(ctx, ok); err == nil {
		t.Error("spec update on a now-too-loose rollout was accepted")
	}
}

func waitForServer(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("webhook server at %s did not start", addr)
}

func ptr[T any](v T) *T { return &v }
