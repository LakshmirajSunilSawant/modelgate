package capture

import (
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

type memPub struct {
	mu     sync.Mutex
	events []analysis.Event
}

func (m *memPub) Publish(e analysis.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
}

// wait returns the published events once n have arrived. Events are emitted
// when the proxy closes the upstream body, which can race the client read.
func (m *memPub) wait(t *testing.T, n int) []analysis.Event {
	t.Helper()
	for i := 0; i < 200; i++ {
		m.mu.Lock()
		got := append([]analysis.Event(nil), m.events...)
		m.mu.Unlock()
		if len(got) >= n {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d events", n)
	return nil
}

func newTestProxy(t *testing.T, body string, status int) (*httptest.Server, *memPub) {
	t.Helper()
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(model.Close)
	u, _ := url.Parse(model.URL)
	pub := &memPub{}
	max := 1.0
	p, err := NewProxy(Config{
		Upstream: u, Rollout: "ns/fraud", Revision: "abc", SampleRate: 1,
		LabelPath: "$.prediction.label", ScorePath: "$.prediction.confidence", ScoreMax: &max,
	}, pub)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return srv, pub
}

func do(t *testing.T, url string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"x":1}`))
	req.Header.Set("X-Request-Id", "r1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func TestProxyExtractsPrediction(t *testing.T) {
	body := `{"prediction":{"label":"fraud","confidence":0.93}}`
	srv, pub := newTestProxy(t, body, 200)
	if got := do(t, srv.URL); got != body {
		t.Fatalf("body altered: %q", got)
	}
	e := pub.wait(t, 1)[0]
	if e.RequestID != "r1" || e.Rollout != "ns/fraud" || e.Revision != "abc" || e.Status != 200 {
		t.Errorf("bad event %+v", e)
	}
	if e.Label == nil || *e.Label != "fraud" || e.Score == nil || *e.Score != 0.93 || e.Invalid != "" {
		t.Errorf("bad prediction %+v", e)
	}
}

func TestProxyMarksInvalid(t *testing.T) {
	cases := map[string]string{
		`{"prediction":{"label":"fraud","confidence":NaN}}`: "unparseable response body",
		`{"prediction":{"label":"fraud","confidence":1.7}}`: "score out of range",
		`{"prediction":{"confidence":0.4}}`:                 "label not found at $.prediction.label",
	}
	for body, want := range cases {
		srv, pub := newTestProxy(t, body, 200)
		do(t, srv.URL)
		if got := pub.wait(t, 1)[0].Invalid; got != want {
			t.Errorf("%s: invalid=%q want %q", body, got, want)
		}
	}
}

func TestProxyRecordsErrors(t *testing.T) {
	srv, pub := newTestProxy(t, `oops`, 503)
	do(t, srv.URL)
	if e := pub.wait(t, 1)[0]; !e.IsError() || e.Label != nil {
		t.Errorf("bad event %+v", e)
	}
}

func TestProxyUpstreamDown(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:1")
	pub := &memPub{}
	p, _ := NewProxy(Config{Upstream: u, Rollout: "ns/x", Revision: "r", SampleRate: 1}, pub)
	srv := httptest.NewServer(p)
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if e := pub.wait(t, 1)[0]; resp.StatusCode != 502 || e.Status != 502 {
		t.Errorf("status %d event %+v", resp.StatusCode, e)
	}
}

func TestParsePath(t *testing.T) {
	p, err := ParsePath(`$.outputs[0]['data'][1]`)
	if err != nil {
		t.Fatal(err)
	}
	doc := map[string]any{"outputs": []any{map[string]any{"data": []any{0.1, 0.9}}}}
	v, ok := p.Get(doc)
	if !ok || v.(float64) != 0.9 {
		t.Errorf("got %v %v", v, ok)
	}
	for _, bad := range []string{"a.b", "$..x", "$[x]", "$.a["} {
		if _, err := ParsePath(bad); err == nil {
			t.Errorf("%q should not parse", bad)
		}
	}
}

func TestSamplingIsDeterministicByRequestID(t *testing.T) {
	const rate, n = 0.25, 20000
	picked := 0
	for i := 0; i < n; i++ {
		id := newID()
		a, b := sampled(id, rate), sampled(id, rate)
		if a != b {
			t.Fatal("same request ID sampled differently")
		}
		if a {
			picked++
		}
	}
	if got := float64(picked) / n; got < 0.23 || got > 0.27 {
		t.Errorf("sampled share %.3f, want ~%.2f", got, rate)
	}
	if !sampled("x", 1) || sampled("x", 0) {
		t.Error("rate bounds")
	}
}
