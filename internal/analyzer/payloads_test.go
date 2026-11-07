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

func TestPayloadsAPI(t *testing.T) {
	srv := newTestServer(t)
	label := "fraud"
	now := time.Now()
	events := []analysis.Event{
		{Rollout: "ns/m", Revision: "s1", RequestID: "r1", Time: now, Status: 200, Label: &label,
			Payload: &analysis.Payload{Request: json.RawMessage(`{"amount":1}`), Response: json.RawMessage(`{"label":"fraud"}`)}},
		{Rollout: "ns/m", Revision: "c1", RequestID: "r1", Time: now, Status: 200, Label: &label,
			Payload: &analysis.Payload{Request: json.RawMessage(`{"amount":1}`), Omitted: []string{"response: not JSON, so it cannot be redacted"}}},
		{Rollout: "ns/m", Revision: "s1", RequestID: "r2", Time: now, Status: 200, Label: &label},
	}
	body, _ := json.Marshal(events)
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	get := func(q string) []analysis.PayloadRecord {
		t.Helper()
		resp, err := http.Get(srv.URL + "/v1/payloads?" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %s", q, resp.Status)
		}
		var out []analysis.PayloadRecord
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := get("rollout=ns/m&requestId=r1"); len(got) != 2 || got[0].Revision != "c1" || len(got[0].Omitted) != 1 {
		t.Errorf("payloads for r1: %+v", got)
	}
	if got := get("rollout=ns/m&limit=1"); len(got) != 1 {
		t.Errorf("limit ignored: %d payloads", len(got))
	}
	if got := get("rollout=ns/other"); len(got) != 0 {
		t.Errorf("payloads of another rollout: %+v", got)
	}
	for _, bad := range []string{"", "rollout=ns/m&limit=0", "rollout=ns/m&limit=x"} {
		resp, err := http.Get(fmt.Sprintf("%s/v1/payloads?%s", srv.URL, bad))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%q: %s", bad, resp.Status)
		}
	}
}
