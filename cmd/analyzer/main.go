// Command analyzer runs the ModelGate analyzer service.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/analyzer"
)

func main() {
	addr := flag.String("listen", ":8090", "HTTP listen address")
	natsURL := flag.String("nats-url", os.Getenv("MODELGATE_NATS_URL"), "NATS URL; empty disables the NATS consumer")
	bucket := flag.Duration("bucket", 15*time.Second, "sketch time-bucket width")
	retention := flag.Duration("retention", 3*time.Hour, "how long sketches are kept")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("component", "analyzer")
	store := analysis.NewStore(analysis.StoreOptions{Bucket: *bucket, Retention: *retention})
	srv := analyzer.NewServer(&analysis.Engine{Store: store}, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *natsURL != "" {
		if err := srv.ConsumeNATS(ctx, *natsURL); err != nil {
			log.Error("NATS consumer failed to start", "err", err)
			os.Exit(1)
		}
		log.Info("consuming events from NATS", "url", *natsURL)
	}

	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				store.GC()
			}
		}
	}()

	hs := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutdown)
	}()
	log.Info("listening", "addr", *addr)
	if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
}
