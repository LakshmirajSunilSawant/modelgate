package analyzer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	eng := &analysis.Engine{Store: analysis.NewStore(analysis.StoreOptions{})}
	srv := httptest.NewServer(NewServer(eng, slog.New(slog.NewTextHandler(io.Discard, nil))).Handler())
	t.Cleanup(srv.Close)
	return srv
}

// ingest posts n events per revision; canary emits an error for errEvery-th request.
func ingest(t *testing.T, srv *httptest.Server, n, errEvery int) {
	t.Helper()
	var buf bytes.Buffer
	now := time.Now()
	for i := 0; i < n; i++ {
		for _, rev := range []string{"s1", "c1"} {
			status := 200
			if rev == "c1" && errEvery > 0 && i%errEvery == 0 {
				status = 503
			}
			label := "legit"
			e := analysis.Event{Rollout: "ns/m", Revision: rev, RequestID: fmt.Sprintf("%s-%d", rev, i), Time: now, LatencyMs: 10, Status: status}
			if status == 200 {
				e.Label = &label
			}
			b, _ := json.Marshal(e)
			buf.Write(append(b, '\n'))
		}
	}
	resp, err := http.Post(srv.URL+"/v1/events", "application/x-ndjson", &buf)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingest status %d", resp.StatusCode)
	}
}

func TestVerdictAPI(t *testing.T) {
	srv := newTestServer(t)
	ingest(t, srv, 300, 0)
	body, _ := json.Marshal(analysis.Request{
		Rollout: "ns/m", Stable: "s1", Canary: "c1", Since: time.Now().Add(-time.Minute),
		Phase: "canary", MinSamples: 200,
		Checks: []analysis.CheckSpec{{Name: "errors", Type: "errorRate"}, {Name: "mix", Type: "psi"}},
	})
	resp, err := http.Post(srv.URL+"/v1/verdict", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var v analysis.Verdict
	json.NewDecoder(resp.Body).Decode(&v)
	if v.Decision != analysis.Pass {
		t.Fatalf("decision %s: %s", v.Decision, v.Reason)
	}
}

func TestArgoAndFlaggerAdapters(t *testing.T) {
	srv := newTestServer(t)
	ingest(t, srv, 300, 4) // 25% canary errors
	q := url.Values{
		"rollout": {"ns/m"}, "stable": {"s1"}, "canary": {"c1"}, "window": {"5m"},
		"minSamples": {"200"}, "checks": {"errors:errorRate:max=0.01,mix:psi:max=0.1"},
	}
	resp, err := http.Get(srv.URL + "/v1/argo/verdict?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	var v analysis.Verdict
	json.NewDecoder(resp.Body).Decode(&v)
	resp.Body.Close()
	if v.Decision != analysis.Fail || !v.HardFailure {
		t.Fatalf("argo: decision %s hard=%v: %s", v.Decision, v.HardFailure, v.Reason)
	}

	payload, _ := json.Marshal(map[string]any{
		"name": "m", "namespace": "ns", "phase": "Progressing",
		"metadata": map[string]string{"stable": "s1", "canary": "c1", "window": "5m", "minSamples": "200", "checks": "errors:errorRate:max=0.01"},
	})
	resp, err = http.Post(srv.URL+"/v1/flagger/check", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("flagger status %d", resp.StatusCode)
	}
}

func TestParseChecks(t *testing.T) {
	cs, err := ParseChecks("a:agreementRate:min=0.97:phase=shadow, b:p99Ratio:max=1.2")
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 2 || *cs[0].Min != 0.97 || cs[0].Phase != "shadow" || *cs[1].Max != 1.2 {
		t.Errorf("parsed %+v", cs)
	}
	if _, err := ParseChecks("a:psi:wat=1"); err == nil {
		t.Error("expected error")
	}
}
