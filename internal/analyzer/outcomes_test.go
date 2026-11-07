package analyzer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
)

func TestOutcomesAPIFeedsLabelAccuracy(t *testing.T) {
	srv := newTestServer(t)
	ingest(t, srv, 300, 0) // every prediction is "legit"

	// Stable was right on every labeled request, canary on none.
	var outcomes []analysis.Outcome
	for i := 0; i < 300; i++ {
		outcomes = append(outcomes,
			analysis.Outcome{Rollout: "ns/m", RequestID: fmt.Sprintf("s1-%d", i), Label: "legit"},
			analysis.Outcome{Rollout: "ns/m", RequestID: fmt.Sprintf("c1-%d", i), Label: "fraud"})
	}
	body, _ := json.Marshal(outcomes)
	resp, err := http.Post(srv.URL+"/v1/outcomes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var ack map[string]int
	_ = json.NewDecoder(resp.Body).Decode(&ack)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || ack["scored"] != 600 {
		t.Fatalf("status %d, ack %v", resp.StatusCode, ack)
	}

	checks, err := ParseChecks("accuracy:labelAccuracy:max=0.02:minSamples=100")
	if err != nil {
		t.Fatal(err)
	}
	req, _ := json.Marshal(analysis.Request{Rollout: "ns/m", Stable: "s1", Canary: "c1",
		Since: time.Now().Add(-time.Minute), Phase: analysis.PhaseCanary, Look: 1, MinSamples: 100, Checks: checks})
	resp, err = http.Post(srv.URL+"/v1/verdict", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var v analysis.Verdict
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		t.Fatal(err)
	}
	if v.Decision != analysis.Fail {
		t.Fatalf("verdict %s: %s", v.Decision, v.Reason)
	}
}

func TestOutcomesAPIRejectsGarbage(t *testing.T) {
	srv := newTestServer(t)
	resp, err := http.Post(srv.URL+"/v1/outcomes", "application/json", bytes.NewReader([]byte("{not json")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", resp.StatusCode)
	}
}
