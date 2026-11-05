// Command sidecar runs the capture proxy in front of a model server. It is
// configured entirely through environment variables set by the controller.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/capture"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		"component", "sidecar", "rollout", os.Getenv("MODELGATE_ROLLOUT"), "revision", os.Getenv("MODELGATE_REVISION"))

	upstream, err := url.Parse(env("MODELGATE_UPSTREAM", "http://127.0.0.1:8080"))
	if err != nil {
		log.Error("bad MODELGATE_UPSTREAM", "err", err)
		os.Exit(1)
	}
	cfg := capture.Config{
		Upstream:   upstream,
		Rollout:    os.Getenv("MODELGATE_ROLLOUT"),
		Revision:   os.Getenv("MODELGATE_REVISION"),
		LabelPath:  os.Getenv("MODELGATE_LABEL_PATH"),
		ScorePath:  os.Getenv("MODELGATE_SCORE_PATH"),
		SampleRate: envFloat("MODELGATE_SAMPLE_RATE", 1),
		ScoreMin:   envFloatPtr("MODELGATE_SCORE_MIN"),
		ScoreMax:   envFloatPtr("MODELGATE_SCORE_MAX"),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var sink capture.Sink
	if natsURL := os.Getenv("MODELGATE_NATS_URL"); natsURL != "" {
		s, nc, err := capture.NewNATSSink(natsURL, cfg.Rollout)
		if err != nil {
			// Fail open: keep serving, events are dropped, the rollout pauses.
			log.Error("NATS unavailable; events will be dropped", "err", err)
			sink = dropSink{}
		} else {
			defer nc.Close()
			sink = s
		}
	} else {
		sink = &capture.HTTPSink{URL: env("MODELGATE_ANALYZER_URL", "http://modelgate-analyzer.modelgate-system.svc/v1/events"),
			Client: &http.Client{Timeout: 5 * time.Second}}
	}
	batcher := capture.NewBatcher(sink, 10000, 500, time.Second, log)
	go batcher.Run(ctx)

	proxy, err := capture.NewProxy(cfg, batcher)
	if err != nil {
		log.Error("bad capture config", "err", err)
		os.Exit(1)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(proxy.Collectors()...)
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "modelgate_sidecar_dropped_total", Help: "Events dropped because the queue was full."},
		func() float64 { return float64(batcher.Dropped.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "modelgate_sidecar_delivery_failed_total", Help: "Events lost to delivery errors."},
		func() float64 { return float64(batcher.Failed.Load()) }))
	admin := http.NewServeMux()
	admin.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	admin.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	servers := []*http.Server{
		{Addr: env("MODELGATE_LISTEN", ":8099"), Handler: proxy, ReadHeaderTimeout: 10 * time.Second},
		{Addr: env("MODELGATE_ADMIN_LISTEN", ":8098"), Handler: admin, ReadHeaderTimeout: 10 * time.Second},
	}
	for _, s := range servers {
		go func(s *http.Server) {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("server failed", "addr", s.Addr, "err", err)
				stop()
			}
		}(s)
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdown)
	}
}

type dropSink struct{}

func (dropSink) Send(context.Context, []analysis.Event) error { return nil }

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if p := envFloatPtr(k); p != nil {
		return *p
	}
	return def
}

func envFloatPtr(k string) *float64 {
	v := os.Getenv(k)
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil
	}
	return &f
}
