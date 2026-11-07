// Command manager runs the ModelGate controller.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/controller"
	mgwebhook "github.com/modelgate/modelgate/internal/webhook"
)

func main() {
	var (
		metricsAddr   = flag.String("metrics-bind-address", ":8080", "metrics endpoint")
		probeAddr     = flag.String("health-probe-bind-address", ":8081", "health probe endpoint")
		leaderElect   = flag.Bool("leader-elect", true, "enable leader election so only one replica reconciles")
		analyzerURL   = flag.String("analyzer-url", "http://modelgate-analyzer.modelgate-system.svc", "analyzer base URL")
		natsURL       = flag.String("nats-url", "", "NATS URL for sidecars; empty sends events to the analyzer over HTTP")
		sidecarImage  = flag.String("sidecar-image", "ghcr.io/modelgate/modelgate-sidecar:latest", "capture sidecar image")
		killSwitchNS  = flag.String("kill-switch-namespace", "modelgate-system", "namespace of the kill-switch ConfigMap")
		killSwitchCM  = flag.String("kill-switch-configmap", "modelgate-config", "name of the kill-switch ConfigMap")
		sidecarIngest = flag.String("sidecar-analyzer-url", "", "analyzer URL as seen from model pods (defaults to --analyzer-url)")
		webhooks      = flag.Bool("enable-webhooks", false, "serve the ModelRollout validating webhook")
		webhookPort   = flag.Int("webhook-port", 9443, "webhook server port")
		webhookCerts  = flag.String("webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "directory with tls.crt and tls.key")
	)
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	must(clientgoscheme.AddToScheme(scheme))
	must(mgv1.AddToScheme(scheme))
	must(gwv1.Install(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress:        *probeAddr,
		LeaderElection:                *leaderElect,
		LeaderElectionID:              "modelgate-controller.modelgate.dev",
		LeaderElectionReleaseOnCancel: true,
		WebhookServer:                 webhook.NewServer(webhook.Options{Port: *webhookPort, CertDir: *webhookCerts}),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	ingest := *sidecarIngest
	if ingest == "" {
		ingest = *analyzerURL
	}
	r := &controller.ModelRolloutReconciler{
		Client:     mgr.GetClient(),
		Scheme:     mgr.GetScheme(),
		Recorder:   mgr.GetEventRecorderFor("modelgate"),
		Analyzer:   &controller.HTTPAnalyzer{BaseURL: *analyzerURL},
		Sidecar:    controller.SidecarConfig{Image: *sidecarImage, AnalyzerURL: ingest + "/v1/events", NATSURL: *natsURL},
		KillSwitch: types.NamespacedName{Namespace: *killSwitchNS, Name: *killSwitchCM},
		APIReader:  mgr.GetAPIReader(),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller")
		os.Exit(1)
	}
	if *webhooks {
		if err := mgwebhook.Setup(mgr); err != nil {
			setupLog.Error(err, "unable to create webhook")
			os.Exit(1)
		}
	}
	must(mgr.AddHealthzCheck("healthz", healthz.Ping))
	must(mgr.AddReadyzCheck("readyz", healthz.Ping))

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
