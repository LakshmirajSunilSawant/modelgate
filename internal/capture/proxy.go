package capture

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/modelgate/modelgate/internal/analysis"
)

// Config configures the capture proxy.
type Config struct {
	Upstream        *url.URL
	Rollout         string // "namespace/name"
	Revision        string
	LabelPath       string
	ScorePath       string
	SampleRate      float64 // share of requests captured, 0..1
	ScoreMin        *float64
	ScoreMax        *float64
	MaxBodyBytes    int64  // responses larger than this are not parsed
	RequestIDHeader string // default x-request-id
}

// Publisher ships events off the request path. Publish must never block.
type Publisher interface {
	Publish(analysis.Event)
}

// Metrics counts what the proxy did.
type Metrics struct {
	Requests prometheus.Counter
	Captured *prometheus.CounterVec
}

func newMetrics() *Metrics {
	return &Metrics{
		Requests: prometheus.NewCounter(prometheus.CounterOpts{Name: "modelgate_sidecar_requests_total", Help: "Requests proxied."}),
		Captured: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "modelgate_sidecar_captured_total", Help: "Captured events by outcome."}, []string{"outcome"}),
	}
}

// Proxy is an http.Handler that forwards to the model server and records
// predictions for a sampled share of requests. It fails open: any capture
// problem affects only the event, never the response.
type Proxy struct {
	cfg     Config
	label   *Path
	score   *Path
	pub     Publisher
	rp      *httputil.ReverseProxy
	Metrics *Metrics
}

type ctxKey struct{}

type capture struct {
	start time.Time
	id    string
}

// NewProxy builds the proxy.
func NewProxy(cfg Config, pub Publisher) (*Proxy, error) {
	if cfg.Upstream == nil {
		return nil, fmt.Errorf("upstream is required")
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 1 << 20
	}
	if cfg.RequestIDHeader == "" {
		cfg.RequestIDHeader = "X-Request-Id"
	}
	label, err := ParsePath(cfg.LabelPath)
	if err != nil {
		return nil, err
	}
	score, err := ParsePath(cfg.ScorePath)
	if err != nil {
		return nil, err
	}
	p := &Proxy{cfg: cfg, label: label, score: score, pub: pub, Metrics: newMetrics()}

	// The upstream is one local model server, so keep enough idle
	// connections for full concurrency; the default of 2 churns connections
	// (and ephemeral ports) at high request rates.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = 512
	transport.MaxIdleConnsPerHost = 512

	p.rp = &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(cfg.Upstream)
			pr.Out.Host = pr.In.Host
			if pr.In.Context().Value(ctxKey{}) != nil {
				// Ask for an uncompressed body so the prediction can be read.
				pr.Out.Header.Del("Accept-Encoding")
			}
		},
		ModifyResponse: p.modifyResponse,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if c, ok := r.Context().Value(ctxKey{}).(*capture); ok {
				p.emit(c, http.StatusBadGateway, nil, "")
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return p, nil
}

// Collectors returns the proxy's metrics for registration.
func (p *Proxy) Collectors() []prometheus.Collector {
	return []prometheus.Collector{p.Metrics.Requests, p.Metrics.Captured}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.Metrics.Requests.Inc()
	id := r.Header.Get(p.cfg.RequestIDHeader)
	if id == "" {
		id = newID()
		r.Header.Set(p.cfg.RequestIDHeader, id)
	}
	if sampled(id, p.cfg.SampleRate) {
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, &capture{start: time.Now(), id: id}))
	}
	p.rp.ServeHTTP(w, r)
}

func (p *Proxy) modifyResponse(resp *http.Response) error {
	c, ok := resp.Request.Context().Value(ctxKey{}).(*capture)
	if !ok {
		return nil
	}
	if resp.StatusCode >= 300 || resp.Header.Get("Content-Encoding") != "" {
		resp.Body = &onClose{ReadCloser: resp.Body, fn: func() { p.emit(c, resp.StatusCode, nil, "") }}
		return nil
	}
	resp.Body = &teeBody{src: resp.Body, limit: p.cfg.MaxBodyBytes, done: func(body []byte, overflow bool) {
		if overflow {
			p.emit(c, resp.StatusCode, nil, "response too large to parse")
			return
		}
		p.emit(c, resp.StatusCode, body, "")
	}}
	return nil
}

// emit builds and publishes the event. It runs after the response has been
// streamed to the client, so parsing never delays the caller.
func (p *Proxy) emit(c *capture, status int, body []byte, invalid string) {
	e := analysis.Event{
		Rollout:   p.cfg.Rollout,
		Revision:  p.cfg.Revision,
		RequestID: c.id,
		Time:      c.start,
		LatencyMs: float64(time.Since(c.start).Microseconds()) / 1000,
		Status:    status,
		Invalid:   invalid,
	}
	if body != nil && invalid == "" && status < 300 {
		p.extract(body, &e)
	}
	outcome := "ok"
	switch {
	case e.IsError():
		outcome = "error"
	case e.Invalid != "":
		outcome = "invalid"
	}
	p.Metrics.Captured.WithLabelValues(outcome).Inc()
	p.pub.Publish(e)
}

func (p *Proxy) extract(body []byte, e *analysis.Event) {
	if p.label == nil && p.score == nil {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		e.Invalid = "unparseable response body" // includes bare NaN, which JSON forbids
		return
	}
	if p.label != nil {
		v, ok := p.label.Get(doc)
		if !ok {
			e.Invalid = "label not found at " + p.label.String()
			return
		}
		l, ok := labelString(v)
		if !ok {
			e.Invalid = "label is not a scalar"
			return
		}
		e.Label = &l
	}
	if p.score != nil {
		v, ok := p.score.Get(doc)
		if !ok {
			e.Invalid = "score not found at " + p.score.String()
			return
		}
		f, ok := scoreFloat(v)
		switch {
		case !ok:
			e.Invalid = "score is not numeric"
		case math.IsNaN(f) || math.IsInf(f, 0):
			e.Invalid = "score is NaN or infinite"
		case p.cfg.ScoreMin != nil && f < *p.cfg.ScoreMin, p.cfg.ScoreMax != nil && f > *p.cfg.ScoreMax:
			e.Invalid = "score out of range"
		default:
			e.Score = &f
		}
		if e.Invalid != "" {
			e.Label = nil
		}
	}
}

// sampled decides capture from the request ID alone, so the stable and
// canary sidecars pick the same requests and shadow pairs survive sampling.
func sampled(id string, rate float64) bool {
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}
	h := fnv.New64a()
	h.Write([]byte(id))
	return float64(h.Sum64()>>11)/(1<<53) < rate
}

func newID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// teeBody passes the body through to the client while keeping a bounded copy.
type teeBody struct {
	src      io.ReadCloser
	buf      bytes.Buffer
	limit    int64
	overflow bool
	done     func([]byte, bool)
	fired    bool
}

func (t *teeBody) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 && !t.overflow {
		if int64(t.buf.Len()+n) > t.limit {
			t.overflow = true
			t.buf.Reset()
		} else {
			t.buf.Write(p[:n])
		}
	}
	return n, err
}

func (t *teeBody) Close() error {
	err := t.src.Close()
	if !t.fired {
		t.fired = true
		t.done(t.buf.Bytes(), t.overflow)
	}
	return err
}

type onClose struct {
	io.ReadCloser
	fn    func()
	fired bool
}

func (o *onClose) Close() error {
	err := o.ReadCloser.Close()
	if !o.fired {
		o.fired = true
		o.fn()
	}
	return err
}
