package capture

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
)

func mustRedactPaths(t *testing.T, exprs ...string) []*Path {
	t.Helper()
	p, err := ParseRedactPaths(exprs)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRedactPaths(t *testing.T) {
	cfg := &PayloadConfig{MaxBytes: 4096, Redact: mustRedactPaths(t,
		"$.instances[*].ssn", "$.patient.*", "$['card number']", "$.rows[1]", "$.missing.field")}
	body := `{"instances":[{"ssn":"111","age":40},{"ssn":"222","age":51}],
		"patient":{"name":"A","dob":"1990"},"card number":"4111","rows":[1,2,3],"keep":true}`
	out, why := redactBody([]byte(body), false, cfg)
	if why != "" {
		t.Fatal(why)
	}
	got := string(out)
	for _, secret := range []string{"111", "222", `"A"`, "1990", "4111", `[1,2,3]`} {
		if strings.Contains(got, secret) {
			t.Errorf("%s survived redaction: %s", secret, got)
		}
	}
	for _, kept := range []string{`"age":40`, `"age":51`, `"keep":true`, `"rows":[1,"[REDACTED]",3]`} {
		if !strings.Contains(got, kept) {
			t.Errorf("%s missing from %s", kept, got)
		}
	}
}

func TestBodiesThatCannotBeRedactedAreLeftOut(t *testing.T) {
	cfg := &PayloadConfig{MaxBytes: 32, Redact: mustRedactPaths(t, "$.ssn")}
	for name, tc := range map[string]struct {
		body     string
		overflow bool
	}{
		"not JSON":       {body: "ssn=123"},
		"trailing data":  {body: `{"a":1} {"ssn":"123"}`},
		"truncated":      {body: `{"ssn":"12`},
		"read overflow":  {body: `{"a":1}`, overflow: true},
		"too long after": {body: `{"ssn":"1","b":"aaaaaaaaaaaaaaaaaaaaa"}`},
	} {
		out, why := redactBody([]byte(tc.body), tc.overflow, cfg)
		if out != nil || why == "" {
			t.Errorf("%s: kept %q (why %q)", name, out, why)
		}
	}
	if out, why := redactBody(nil, false, cfg); out != nil || why != "" {
		t.Errorf("empty body: %q %q", out, why)
	}
}

func TestRedactPathsMustNameAField(t *testing.T) {
	for _, bad := range []string{"$", "ssn", "$.a[", "$.a[x]"} {
		if _, err := ParseRedactPaths([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestWildcardsOnlyInRedactPaths(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:1")
	if _, err := NewProxy(Config{Upstream: u, LabelPath: "$.predictions[*].label"}, &memPub{}); err == nil {
		t.Error("wildcard label path accepted")
	}
}

// payloadProxy runs a model that echoes the request field "x" and returns a
// label, behind a proxy capturing payloads.
func payloadProxy(t *testing.T, pc PayloadConfig) (*httptest.Server, *memPub, func() []string) {
	t.Helper()
	var (
		mu   sync.Mutex
		seen []string
	)
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, string(b))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"prediction":{"label":"fraud","confidence":0.9},"echo":{"ssn":"123-45"}}`)
	}))
	t.Cleanup(model.Close)
	u, _ := url.Parse(model.URL)
	pub := &memPub{}
	p, err := NewProxy(Config{Upstream: u, Rollout: "ns/fraud", Revision: "abc", SampleRate: 1,
		LabelPath: "$.prediction.label", Payloads: pc}, pub)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv, pub, func() []string { mu.Lock(); defer mu.Unlock(); return append([]string(nil), seen...) }
}

func post(t *testing.T, url, id, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("X-Request-Id", id)
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func TestProxyCapturesRedactedPayloads(t *testing.T) {
	srv, pub, seen := payloadProxy(t, PayloadConfig{Until: time.Now().Add(time.Hour), SampleRate: 1, MaxBytes: 4096,
		Redact: mustRedactPaths(t, "$.ssn", "$.echo.ssn")})
	body := `{"ssn":"123-45","amount":10}`
	post(t, srv.URL, "r1", body)
	e := pub.wait(t, 1)[0]
	if seen()[0] != body {
		t.Errorf("model got %q, want the unmodified body", seen()[0])
	}
	if e.Label == nil || *e.Label != "fraud" {
		t.Errorf("prediction lost: %+v", e)
	}
	if e.Payload == nil {
		t.Fatal("no payload captured")
	}
	all, _ := json.Marshal(e.Payload)
	if strings.Contains(string(all), "123-45") || strings.Contains(string(all), "secret-token") {
		t.Errorf("secret in payload: %s", all)
	}
	if !strings.Contains(string(e.Payload.Request), `"amount":10`) || !strings.Contains(string(e.Payload.Response), `"label":"fraud"`) {
		t.Errorf("payload %s", all)
	}
}

func TestPayloadCaptureStopsAtDeadline(t *testing.T) {
	srv, pub, _ := payloadProxy(t, PayloadConfig{Until: time.Now().Add(-time.Second), SampleRate: 1, MaxBytes: 4096})
	post(t, srv.URL, "r1", `{"a":1}`)
	if e := pub.wait(t, 1)[0]; e.Payload != nil {
		t.Errorf("payload captured after the deadline: %+v", e.Payload)
	}
}

func TestPayloadSamplingMatchesAcrossRevisions(t *testing.T) {
	// The payload set is a subset of the capture set and is chosen by request
	// ID alone, so a stable and a canary sidecar keep the same requests.
	for i := 0; i < 2000; i++ {
		id := newID()
		if sampled(id, 0.05) && !sampled(id, 0.5) {
			t.Fatalf("request %s kept for payloads but not captured", id)
		}
	}
}

func TestPayloadRateLimit(t *testing.T) {
	var l payloadLimiter
	now := time.Unix(1000, 0)
	n := 0
	for i := 0; i < 100; i++ {
		if l.allow(now) {
			n++
		}
	}
	if n != maxPayloadsPerSecond || !l.allow(now.Add(time.Second)) {
		t.Errorf("allowed %d in one second", n)
	}
}

func TestChunksStayUnderLimit(t *testing.T) {
	big := json.RawMessage(`"` + strings.Repeat("x", 3000) + `"`)
	var events []analysis.Event
	for i := 0; i < 10; i++ {
		events = append(events, analysis.Event{Rollout: "ns/r", Revision: "a", Payload: &analysis.Payload{Request: big}})
	}
	events = append(events, analysis.Event{Rollout: "ns/r", Revision: "a",
		Payload: &analysis.Payload{Request: json.RawMessage(`"` + strings.Repeat("y", 20000) + `"`)}})
	chunks, err := encodeChunks(events, 8000)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, c := range chunks {
		if len(c) > 8000 {
			t.Errorf("chunk of %d bytes", len(c))
		}
		var got []analysis.Event
		if err := json.Unmarshal(c, &got); err != nil {
			t.Fatal(err)
		}
		total += len(got)
	}
	if total != len(events) {
		t.Errorf("%d events after chunking, want %d", total, len(events))
	}
}
