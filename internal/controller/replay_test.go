package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/capture"
)

func TestParseReplayRequests(t *testing.T) {
	got, err := parseReplayRequests("{\"a\":1}\n\n  [1,2]  \r\n{\"b\":\"x\"}")
	if err != nil || len(got) != 3 || string(got[1]) != "[1,2]" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := parseReplayRequests("{\"a\":1}\n{bad"); err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("bad line: %v", err)
	}
	if _, err := parseReplayRequests("\n\n"); err == nil {
		t.Error("empty data accepted")
	}
}

type replayHit struct{ svc, path, id, body string }

// replayEnv is a rollout in its shadow stage with replay configured, and a
// test server standing in for the stable and canary Services.
func replayEnv(t *testing.T) (*env, *Replayer, func() []replayHit) {
	e := newEnv(t)
	e.startRollout()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: "fraud-replay"},
		Data:       map[string]string{"requests.jsonl": "{\"x\":1}\n{\"x\":2}\n"},
	}
	if err := e.c.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.Strategy.Replay = &mgv1.ReplaySpec{ConfigMapRef: mgv1.ConfigMapKeyRef{Name: "fraud-replay"}, Path: "/predict", RatePerSecond: 30}
		mr.Spec.Capture.SampleRate = 0.5
	})
	e.reconcile()

	var mu sync.Mutex
	var hits []replayHit
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		svc, path, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		mu.Lock()
		hits = append(hits, replayHit{svc: svc, path: "/" + path, id: r.Header.Get("X-Request-Id"), body: string(b)})
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	rp := &Replayer{Client: e.c, Reader: e.c, BaseURL: func(ns, svc string) string { return srv.URL + "/" + svc }}
	return e, rp, func() []replayHit {
		mu.Lock()
		defer mu.Unlock()
		out := hits
		hits = nil
		return out
	}
}

func TestReplaySendsEachRequestToBothVersions(t *testing.T) {
	e, rp, hits := replayEnv(t)
	if c := findCondition(e.get(), mgv1.ConditionReplay); c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, "2 recorded requests") {
		t.Fatalf("Replay condition %+v", c)
	}
	rp.RunOnce(context.Background(), 100*time.Millisecond) // 30/s for 0.1s: 3 requests
	got := hits()
	if len(got) != 6 {
		t.Fatalf("%d hits, want 3 requests to each version: %+v", len(got), got)
	}
	byID := map[string][]replayHit{}
	for _, h := range got {
		if h.path != "/predict" {
			t.Errorf("path %s", h.path)
		}
		if !capture.Sampled(h.id, 0.5) {
			t.Errorf("request ID %s is not captured at the rollout's sample rate", h.id)
		}
		byID[h.id] = append(byID[h.id], h)
	}
	if len(byID) != 3 {
		t.Fatalf("%d distinct IDs, want 3", len(byID))
	}
	for id, hs := range byID {
		if len(hs) != 2 || hs[0].svc == hs[1].svc || hs[0].body != hs[1].body {
			t.Errorf("request %s was not sent once to each version with the same body: %+v", id, hs)
		}
	}

	// The next tick carries on through the recorded requests with new IDs.
	rp.RunOnce(context.Background(), 100*time.Millisecond)
	for _, h := range hits() {
		if byID[h.id] != nil {
			t.Errorf("request ID %s reused", h.id)
		}
	}
}

func TestReplayStopsWhenHeldOrDone(t *testing.T) {
	e, rp, hits := replayEnv(t)
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Paused = true })
	e.reconcile()
	rp.RunOnce(context.Background(), 100*time.Millisecond)
	if n := len(hits()); n != 0 {
		t.Errorf("%d requests replayed while paused", n)
	}

	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Paused = false })
	e.reconcile()
	rp.RunOnce(context.Background(), 100*time.Millisecond)
	if n := len(hits()); n == 0 {
		t.Error("no requests replayed after resume")
	}

	e.update(func(mr *mgv1.ModelRollout) { mr.Annotations = map[string]string{mgv1.ActionAnnotation: "abort"} })
	e.reconcile()
	rp.RunOnce(context.Background(), 100*time.Millisecond)
	if n := len(hits()); n != 0 {
		t.Errorf("%d requests replayed after the canary was rolled back", n)
	}
}

func TestReplayConditionReportsBadConfigMaps(t *testing.T) {
	e, rp, hits := replayEnv(t)
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.ConfigMap) bool // false deletes the ConfigMap
		reason string
	}{
		{"wrong key", func(cm *corev1.ConfigMap) bool { cm.Data = map[string]string{"other": "{}"}; return true }, replayNoKey},
		{"bad JSON", func(cm *corev1.ConfigMap) bool { cm.Data = map[string]string{"requests.jsonl": "{"}; return true }, replayBadRequests},
		{"missing", func(*corev1.ConfigMap) bool { return false }, replayNoConfigMap},
	} {
		cm := &corev1.ConfigMap{}
		if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: "ml", Name: "fraud-replay"}, cm); err != nil {
			t.Fatal(err)
		}
		var err error
		if tc.mutate(cm) {
			err = e.c.Update(context.Background(), cm)
		} else {
			err = e.c.Delete(context.Background(), cm)
		}
		if err != nil {
			t.Fatal(err)
		}
		e.reconcile()
		if c := findCondition(e.get(), mgv1.ConditionReplay); c == nil || c.Status != metav1.ConditionFalse || c.Reason != tc.reason {
			t.Errorf("%s: condition %+v, want %s", tc.name, c, tc.reason)
		}
		rp.RunOnce(context.Background(), 100*time.Millisecond)
		if n := len(hits()); n != 0 {
			t.Errorf("%s: %d requests replayed", tc.name, n)
		}
	}
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Strategy.Replay = nil })
	e.reconcile()
	if c := findCondition(e.get(), mgv1.ConditionReplay); c != nil {
		t.Errorf("condition left after removing replay: %+v", c)
	}
}
