// Command loadtest measures the latency the capture sidecar adds. It runs a
// stand-in model server (fixed service time, JSON response), then drives an
// open-loop load at a fixed rate twice: straight to the model, and through
// the capture proxy with capture on for every request. Events go through
// the real batcher into a discarding sink.
//
//	go run ./hack/loadtest -rps 1000 -duration 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/capture"
)

type discard struct{ n atomic.Int64 }

func (d *discard) Send(_ context.Context, ev []analysis.Event) error {
	d.n.Add(int64(len(ev)))
	return nil
}

func serve(h http.Handler) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go (&http.Server{Handler: h}).Serve(ln)
	return "http://" + ln.Addr().String()
}

func main() {
	rps := flag.Int("rps", 1000, "requests per second")
	duration := flag.Duration("duration", 30*time.Second, "duration per pass")
	service := flag.Duration("service", 5*time.Millisecond, "model service time")
	flag.Parse()

	body := `{"prediction":{"label":"benign","confidence":0.8731}}`
	model := serve(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		time.Sleep(*service)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}))
	u, _ := url.Parse(model)
	sink := &discard{}
	b := capture.NewBatcher(sink, 10000, 500, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	go b.Run(ctx)
	p, err := capture.NewProxy(capture.Config{
		Upstream: u, Rollout: "bench/m", Revision: "r1", SampleRate: 1,
		LabelPath: "$.prediction.label", ScorePath: "$.prediction.confidence",
	}, b)
	if err != nil {
		panic(err)
	}
	proxied := serve(p)

	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1000, MaxConnsPerHost: 0}, Timeout: 10 * time.Second}
	run := func(target string) []float64 {
		// Warm up connections, then measure.
		drive(client, target, *rps, 2*time.Second)
		return drive(client, target, *rps, *duration)
	}
	direct := run(model)
	viaSidecar := run(proxied)
	time.Sleep(1500 * time.Millisecond)
	cancel()

	fmt.Printf("open-loop %d req/s for %s per pass, model service time %s, capture on every request\n\n", *rps, *duration, *service)
	fmt.Println("| Path | Requests | p50 | p90 | p99 | p99.9 |")
	fmt.Println("| --- | --- | --- | --- | --- | --- |")
	row := func(name string, xs []float64) {
		fmt.Printf("| %s | %d | %.2f ms | %.2f ms | %.2f ms | %.2f ms |\n", name, len(xs), q(xs, .5), q(xs, .9), q(xs, .99), q(xs, .999))
	}
	row("direct", direct)
	row("via sidecar", viaSidecar)
	fmt.Printf("\nadded p50 %.2f ms, added p99 %.2f ms; events captured %d, dropped %d, delivery failures %d\n",
		q(viaSidecar, .5)-q(direct, .5), q(viaSidecar, .99)-q(direct, .99), sink.n.Load(), b.Dropped.Load(), b.Failed.Load())
	if q(viaSidecar, .99)-q(direct, .99) > 2 {
		os.Exit(1)
	}
}

// drive sends requests on a fixed schedule regardless of responses, so a
// slow path cannot hide its latency by lowering the offered load.
func drive(c *http.Client, target string, rps int, d time.Duration) []float64 {
	var mu sync.Mutex
	var lat []float64
	var wg sync.WaitGroup
	gap := time.Second / time.Duration(rps)
	start := time.Now()
	for i := 0; ; i++ {
		at := start.Add(time.Duration(i) * gap)
		if at.Sub(start) >= d {
			break
		}
		time.Sleep(time.Until(at))
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodPost, target+"/predict", strings.NewReader(`{"features":[1,2,3]}`))
			req.Header.Set("X-Request-Id", fmt.Sprint(id))
			t := time.Now()
			resp, err := c.Do(req)
			if err != nil {
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ms := float64(time.Since(t).Microseconds()) / 1000
			mu.Lock()
			lat = append(lat, ms)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return lat
}

func q(xs []float64, p float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[int(p*float64(len(s)-1))]
}
