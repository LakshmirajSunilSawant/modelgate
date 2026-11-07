// Package analyzer exposes the analysis engine over HTTP: event ingest, the
// verdict API the ModelGate controller uses, and adapters matching the Argo
// Rollouts web metric and Flagger webhook contracts.
package analyzer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/modelgate/modelgate/internal/analysis"
)

// Server is the analyzer HTTP API.
type Server struct {
	Engine *analysis.Engine
	Log    *slog.Logger

	reg       *prometheus.Registry
	ingested  prometheus.Counter
	verdicts  *prometheus.CounterVec
	checkVals *prometheus.GaugeVec
	outcomes  prometheus.Counter
	scored    prometheus.Counter
}

// NewServer wires routes and metrics.
func NewServer(eng *analysis.Engine, log *slog.Logger) *Server {
	s := &Server{Engine: eng, Log: log, reg: prometheus.NewRegistry()}
	s.ingested = prometheus.NewCounter(prometheus.CounterOpts{Name: "modelgate_analyzer_events_total", Help: "Prediction events ingested."})
	s.verdicts = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "modelgate_analyzer_verdicts_total", Help: "Verdicts served."}, []string{"rollout", "decision"})
	s.checkVals = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "modelgate_analyzer_check_value", Help: "Latest value of each check."}, []string{"rollout", "check", "type"})
	s.outcomes = prometheus.NewCounter(prometheus.CounterOpts{Name: "modelgate_analyzer_outcomes_total", Help: "Ground-truth labels received."})
	s.scored = prometheus.NewCounter(prometheus.CounterOpts{Name: "modelgate_analyzer_outcomes_scored_total", Help: "Predictions scored against a ground-truth label."})
	s.reg.MustRegister(s.ingested, s.verdicts, s.checkVals, s.outcomes, s.scored, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return s
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.handleEvents)
	mux.HandleFunc("POST /v1/outcomes", s.handleOutcomes)
	mux.HandleFunc("POST /v1/verdict", s.handleVerdict)
	mux.HandleFunc("GET /v1/payloads", s.handlePayloads)
	mux.HandleFunc("GET /v1/argo/verdict", s.handleArgo)
	mux.HandleFunc("POST /v1/flagger/check", s.handleFlagger)
	mux.HandleFunc("GET /v1/rollouts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, s.Engine.Store.Rollouts())
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	return mux
}

// Ingest adds events and updates metrics. The NATS consumer calls it too.
func (s *Server) Ingest(events []analysis.Event) {
	s.Engine.Store.Ingest(events...)
	s.ingested.Add(float64(len(events)))
}

// DecodeEvents accepts a JSON array or newline-delimited JSON.
func DecodeEvents(body []byte) ([]analysis.Event, error) {
	body = bytes.TrimSpace(body)
	var events []analysis.Event
	if len(body) > 0 && body[0] == '[' {
		err := json.Unmarshal(body, &events)
		return events, err
	}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 64*1024), 1<<20)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e analysis.Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, sc.Err()
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(http.MaxBytesReader(w, r.Body, 16<<20)); err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	events, err := DecodeEvents(buf.Bytes())
	if err != nil {
		http.Error(w, "bad events: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.Ingest(events)
	w.WriteHeader(http.StatusAccepted)
}

// handleOutcomes takes delayed ground-truth labels as a JSON array of
// {"rollout": "ns/name", "requestId": "...", "label": "..."} and joins them
// with the captured predictions for labelAccuracy checks.
func (s *Server) handleOutcomes(w http.ResponseWriter, r *http.Request) {
	var outcomes []analysis.Outcome
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<20)).Decode(&outcomes); err != nil {
		http.Error(w, "bad outcomes: "+err.Error(), http.StatusBadRequest)
		return
	}
	scored := s.Engine.Store.IngestOutcomes(outcomes...)
	s.outcomes.Add(float64(len(outcomes)))
	s.scored.Add(float64(scored))
	writeJSON(w, http.StatusAccepted, map[string]int{"received": len(outcomes), "scored": scored})
}

func (s *Server) evaluate(req analysis.Request) analysis.Verdict {
	v := s.Engine.Evaluate(req)
	s.verdicts.WithLabelValues(req.Rollout, v.Decision).Inc()
	for _, c := range v.Checks {
		if c.Status != analysis.Skipped && c.Status != analysis.Inconclusive {
			s.checkVals.WithLabelValues(req.Rollout, c.Name, c.Type).Set(c.Value)
		}
	}
	s.Log.Info("verdict", "rollout", req.Rollout, "stable", req.Stable, "canary", req.Canary,
		"phase", req.Phase, "look", v.Look, "decision", v.Decision, "reason", v.Reason)
	return v
}

func (s *Server) handleVerdict(w http.ResponseWriter, r *http.Request) {
	var req analysis.Request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := validate(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.evaluate(req))
}

func validate(req *analysis.Request) error {
	if req.Rollout == "" || req.Stable == "" || req.Canary == "" {
		return fmt.Errorf("rollout, stable and canary are required")
	}
	if len(req.Checks) == 0 {
		return fmt.Errorf("at least one check is required")
	}
	return nil
}

// handleArgo serves an Argo Rollouts web metric. Example AnalysisTemplate
// provider:
//
//	web:
//	  url: http://modelgate-analyzer.modelgate-system/v1/argo/verdict?rollout=ns/fraud&stable={{args.stable}}&canary={{args.canary}}&window=10m&minSamples=2000&checks=agreement:agreementRate:min=0.97,psi:psi:max=0.1
//	  jsonPath: "{$.decision}"
//	successCondition: result == "pass"
//	failureCondition: result == "fail"
//
// Inconclusive verdicts satisfy neither condition, so Argo reports the
// measurement as inconclusive and pauses rather than promoting.
// handlePayloads lists redacted payloads captured in the last hour, newest
// first: GET /v1/payloads?rollout=ns/name[&requestId=...][&limit=n].
// Bodies were redacted in the sidecar before they were sent here.
func (s *Server) handlePayloads(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rollout := q.Get("rollout")
	if rollout == "" {
		http.Error(w, "rollout is required", http.StatusBadRequest)
		return
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = min(n, 500)
	}
	writeJSON(w, http.StatusOK, s.Engine.Store.Payloads(rollout, q.Get("requestId"), limit))
}

func (s *Server) handleArgo(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	req, err := requestFromParams(func(k string) string { return q.Get(k) })
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.evaluate(req))
}

// flaggerPayload is the body Flagger sends to webhooks.
type flaggerPayload struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Phase     string            `json:"phase"`
	Metadata  map[string]string `json:"metadata"`
}

// handleFlagger implements a Flagger webhook: HTTP 200 passes the canary
// check, anything else fails it. Parameters come from the webhook metadata
// using the same keys as the Argo query string. Inconclusive verdicts
// return 425 Too Early; Flagger counts them toward its failure threshold, so
// set that threshold with the time to reach minSamples in mind.
func (s *Server) handleFlagger(w http.ResponseWriter, r *http.Request) {
	var p flaggerPayload
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&p); err != nil {
		http.Error(w, "bad payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	get := func(k string) string {
		if k == "rollout" && p.Metadata[k] == "" && p.Name != "" {
			return p.Namespace + "/" + p.Name
		}
		return p.Metadata[k]
	}
	req, err := requestFromParams(get)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	v := s.evaluate(req)
	status := http.StatusOK
	switch v.Decision {
	case analysis.Fail:
		status = http.StatusPreconditionFailed
	case analysis.Inconclusive:
		status = http.StatusTooEarly
	}
	writeJSON(w, status, v)
}

// requestFromParams builds a request from flat string parameters:
// rollout, stable, canary, phase, window (duration) or since (RFC 3339),
// minSamples, alpha, look, and checks as name:type[:min=x|max=y][:phase=p]
// separated by commas.
func requestFromParams(get func(string) string) (analysis.Request, error) {
	req := analysis.Request{
		Rollout: get("rollout"),
		Stable:  get("stable"),
		Canary:  get("canary"),
		Phase:   get("phase"),
	}
	switch {
	case get("since") != "":
		t, err := time.Parse(time.RFC3339, get("since"))
		if err != nil {
			return req, fmt.Errorf("since: %w", err)
		}
		req.Since = t
	default:
		window := 10 * time.Minute
		if ws := get("window"); ws != "" {
			d, err := time.ParseDuration(ws)
			if err != nil {
				return req, fmt.Errorf("window: %w", err)
			}
			window = d
		}
		// Align to the window so repeated calls share a look counter.
		req.Since = time.Now().Add(-window).Truncate(time.Minute)
	}
	var err error
	if v := get("minSamples"); v != "" {
		if req.MinSamples, err = strconv.ParseInt(v, 10, 64); err != nil {
			return req, fmt.Errorf("minSamples: %w", err)
		}
	}
	if v := get("alpha"); v != "" {
		if req.Alpha, err = strconv.ParseFloat(v, 64); err != nil {
			return req, fmt.Errorf("alpha: %w", err)
		}
	}
	if v := get("look"); v != "" {
		if req.Look, err = strconv.Atoi(v); err != nil {
			return req, fmt.Errorf("look: %w", err)
		}
	}
	if req.Checks, err = ParseChecks(get("checks")); err != nil {
		return req, err
	}
	return req, validate(&req)
}

// ParseChecks parses the compact check syntax used by the webhook adapters.
func ParseChecks(s string) ([]analysis.CheckSpec, error) {
	var out []analysis.CheckSpec
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("check %q: want name:type[:min=x|max=y][:phase=p]", item)
		}
		c := analysis.CheckSpec{Name: parts[0], Type: parts[1]}
		for _, kv := range parts[2:] {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return nil, fmt.Errorf("check %q: bad option %q", item, kv)
			}
			switch k {
			case "min", "max":
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, fmt.Errorf("check %q: %s: %w", item, k, err)
				}
				if k == "min" {
					c.Min = &f
				} else {
					c.Max = &f
				}
			case "phase":
				c.Phase = v
			case "minSamples":
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil || n < 1 {
					return nil, fmt.Errorf("check %q: minSamples must be a positive integer", item)
				}
				c.MinSamples = &n
			default:
				return nil, fmt.Errorf("check %q: unknown option %q", item, k)
			}
		}
		out = append(out, c)
	}
	return out, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
