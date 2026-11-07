package analysis

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// labeledRun serves n split-traffic requests with a known true class per
// request. The canary answers correctly with probability canaryAcc and stable
// with stableAcc. A share labelRate of requests later receive their label.
func labeledRun(t *testing.T, seed int64, n int, stableAcc, canaryAcc, labelRate float64, look int) Verdict {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	now := t0.Add(time.Hour)
	store := NewStore(StoreOptions{Now: func() time.Time { return now }})
	var outcomes []Outcome
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("req-%d", i)
		truth := "legit"
		if r.Float64() < 0.3 {
			truth = "fraud"
		}
		rev, acc := "stable", stableAcc
		if i%2 == 1 {
			rev, acc = "canary", canaryAcc
		}
		pred := truth
		if r.Float64() >= acc {
			pred = map[string]string{"legit": "fraud", "fraud": "legit"}[truth]
		}
		store.Ingest(Event{Rollout: "ml/fraud", Revision: rev, RequestID: id, Time: t0.Add(time.Duration(i) * 50 * time.Millisecond),
			LatencyMs: 10, Status: 200, Label: &pred})
		if r.Float64() < labelRate {
			outcomes = append(outcomes, Outcome{Rollout: "ml/fraud", RequestID: id, Label: truth})
		}
	}
	store.IngestOutcomes(outcomes...)
	eng := &Engine{Store: store}
	return eng.Evaluate(Request{Rollout: "ml/fraud", Stable: "stable", Canary: "canary", Since: t0, Phase: PhaseCanary,
		Look: look, MinSamples: 100, Checks: []CheckSpec{{Name: "accuracy", Type: CheckLabelAccuracy}}})
}

func TestLabelAccuracyCatchesDrop(t *testing.T) {
	v := labeledRun(t, 1, 10000, 0.96, 0.90, 0.8, 1) // about 4000 labels per side
	if v.Decision != Fail {
		t.Fatalf("6-point accuracy drop not caught: %s (%+v)", v.Reason, v.Checks)
	}
}

func TestLabelAccuracyWaitsForLabels(t *testing.T) {
	v := labeledRun(t, 2, 8000, 0.96, 0.90, 0.02, 1) // about 80 labels per side
	if v.Decision != Inconclusive {
		t.Fatalf("decided on too few labels: %s", v.Reason)
	}
}

// Equal accuracy must not trip the check more often than the alpha budget
// allows, across repeated looks.
func TestLabelAccuracyFalseAlarms(t *testing.T) {
	fails := 0
	for seed := int64(0); seed < 100; seed++ {
		if v := labeledRun(t, 100+seed, 4000, 0.95, 0.95, 0.5, int(seed%5)+1); v.Decision == Fail {
			fails++
		}
	}
	if fails > 2 {
		t.Fatalf("%d/100 false failures on equally accurate models", fails)
	}
}

func TestOutcomesForUnknownRequestsAreIgnored(t *testing.T) {
	store := NewStore(StoreOptions{})
	l := "fraud"
	store.Ingest(Event{Rollout: "ml/fraud", Revision: "a", RequestID: "r1", Status: 200, Label: &l})
	if n := store.IngestOutcomes(Outcome{Rollout: "ml/fraud", RequestID: "nope", Label: "fraud"},
		Outcome{Rollout: "other/x", RequestID: "r1", Label: "fraud"}); n != 0 {
		t.Fatalf("scored %d unknown outcomes", n)
	}
	if n := store.IngestOutcomes(Outcome{Rollout: "ml/fraud", RequestID: "r1", Label: "fraud"}); n != 1 {
		t.Fatalf("scored %d, want 1", n)
	}
	// A second label for the same request is not double counted.
	if n := store.IngestOutcomes(Outcome{Rollout: "ml/fraud", RequestID: "r1", Label: "legit"}); n != 0 {
		t.Fatalf("label counted twice")
	}
	if ts := store.Track("ml/fraud", "a", time.Time{}); ts.Labeled != 1 || ts.Correct != 1 {
		t.Fatalf("track %+v", ts)
	}
}
