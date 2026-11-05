package analysis

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// model simulates a binary fraud classifier: score = sigmoid(w*x + b).
type model struct {
	w, b      float64
	swap      bool    // label mapping swapped
	scale     float64 // feature preprocessing scale bug (1 = none)
	nanRate   float64
	errRate   float64
	latencyMs float64
	noise     float64 // per-request score noise (a slightly different model)
}

var good = model{w: 3, b: -4, scale: 1, latencyMs: 20}

func (m model) predict(r *rand.Rand, x float64) (label string, score float64, invalid string, status int, latency float64) {
	latency = m.latencyMs * (0.5 + r.ExpFloat64()*0.5)
	if r.Float64() < m.errRate {
		return "", 0, "", 500, latency
	}
	if r.Float64() < m.nanRate {
		return "", 0, "NaN in prediction", 200, latency
	}
	z := m.w*x*m.scale + m.b + m.noise*r.NormFloat64()
	score = 1 / (1 + math.Exp(-z))
	label = "legit"
	if score > 0.5 {
		label = "fraud"
	}
	if m.swap {
		if label == "fraud" {
			label = "legit"
		} else {
			label = "fraud"
		}
	}
	return label, score, "", 200, latency
}

func event(rollout, rev, id string, t time.Time, m model, r *rand.Rand, x float64) Event {
	l, s, inv, st, lat := m.predict(r, x)
	e := Event{Rollout: rollout, Revision: rev, RequestID: id, Time: t, LatencyMs: lat, Status: st, Invalid: inv}
	if st == 200 && inv == "" {
		e.Label, e.Score = &l, &s
	}
	return e
}

type sim struct {
	store *Store
	eng   *Engine
	r     *rand.Rand
	t0    time.Time
	now   time.Time
	n     int
}

func newSim(seed int64) *sim {
	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	s := &sim{r: rand.New(rand.NewSource(seed)), t0: t0, now: t0.Add(time.Hour)}
	s.store = NewStore(StoreOptions{Now: func() time.Time { return s.now }})
	s.eng = &Engine{Store: s.store}
	return s
}

// shadow serves n requests from stable and mirrors a sampleRate share to canary.
func (s *sim) shadow(n int, sampleRate float64, stable, canary model) {
	for i := 0; i < n; i++ {
		s.n++
		id := fmt.Sprintf("req-%d", s.n)
		x := s.r.NormFloat64() + 0.5
		t := s.t0.Add(time.Duration(s.n) * 10 * time.Millisecond)
		s.store.Ingest(event("ns/fraud", "stable", id, t, stable, s.r, x))
		if s.r.Float64() < sampleRate {
			s.store.Ingest(event("ns/fraud", "canary", id, t, canary, s.r, x))
		}
	}
}

// split serves n requests, a weight share by canary.
func (s *sim) split(n int, weight float64, stable, canary model) {
	for i := 0; i < n; i++ {
		s.n++
		id := fmt.Sprintf("req-%d", s.n)
		x := s.r.NormFloat64() + 0.5
		t := s.t0.Add(time.Duration(s.n) * 10 * time.Millisecond)
		if s.r.Float64() < weight {
			s.store.Ingest(event("ns/fraud", "canary", id, t, canary, s.r, x))
		} else {
			s.store.Ingest(event("ns/fraud", "stable", id, t, stable, s.r, x))
		}
	}
}

func f(v float64) *float64 { return &v }

var prdChecks = []CheckSpec{
	{Name: "agreement", Type: CheckAgreementRate, Min: f(0.97), Phase: PhaseShadow},
	{Name: "pred-shift", Type: CheckPSI, Max: f(0.10)},
	{Name: "score-shape", Type: CheckKS, Max: f(0.10)},
	{Name: "valid", Type: CheckValidity},
	{Name: "latency", Type: CheckP99Ratio, Max: f(1.20)},
	{Name: "errors", Type: CheckErrorRate, Max: f(0.01)},
}

func (s *sim) verdict(phase string, look int) Verdict {
	return s.eng.Evaluate(Request{
		Rollout: "ns/fraud", Stable: "stable", Canary: "canary", Since: s.t0,
		Phase: phase, Look: look, MinSamples: 2000, Checks: prdChecks,
	})
}

func TestInconclusiveBeforeMinSamples(t *testing.T) {
	s := newSim(1)
	s.shadow(1000, 0.2, good, good)
	v := s.verdict(PhaseShadow, 1)
	if v.Decision != Inconclusive {
		t.Fatalf("decision %s: %s", v.Decision, v.Reason)
	}
}

func TestGoodModelPasses(t *testing.T) {
	s := newSim(2)
	s.shadow(12000, 0.2, good, good)
	if v := s.verdict(PhaseShadow, 1); v.Decision != Pass {
		t.Fatalf("shadow: %s: %s %+v", v.Decision, v.Reason, v.Checks)
	}
	s2 := newSim(3)
	s2.split(40000, 0.1, good, good)
	if v := s2.verdict(PhaseCanary, 1); v.Decision != Pass {
		t.Fatalf("canary: %s: %s %+v", v.Decision, v.Reason, v.Checks)
	}
}

// TestFalseAlarmRateIdenticalModel runs many identical-model rollouts with
// repeated looks and checks the false rollback rate stays under the PRD's 2%.
func TestFalseAlarmRateIdenticalModel(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	const runs = 50
	falseFails := 0
	noisy := good
	noisy.noise = 0.05 // retrained on the same data: tiny score differences
	for run := 0; run < runs; run++ {
		s := newSim(int64(100 + run))
		failed := false
		// Shadow stage, 6 looks.
		for look := 1; look <= 6 && !failed; look++ {
			s.shadow(3000, 0.2, good, noisy)
			failed = s.verdict(PhaseShadow, look).Decision == Fail
		}
		// One canary stage at 25%, 6 looks, fresh window.
		s.t0 = s.t0.Add(time.Duration(s.n) * 10 * time.Millisecond).Add(time.Minute)
		s.n = 0
		for look := 1; look <= 6 && !failed; look++ {
			s.split(3000, 0.25, good, noisy)
			failed = s.verdict(PhaseCanary, look).Decision == Fail
		}
		if failed {
			falseFails++
		}
	}
	if rate := float64(falseFails) / runs; rate >= 0.02 {
		t.Errorf("false rollback rate %.1f%% on identical models", 100*rate)
	}
}

func TestBadModelsFail(t *testing.T) {
	stale := good
	stale.w, stale.b = 2, -2.5 // trained on last quarter's fraud mix
	cases := map[string]struct {
		m        model
		wantHard bool
		check    string
		split    bool // judge on weighted canary traffic instead of shadow
	}{
		"swapped labels":  {m: model{w: 3, b: -4, scale: 1, latencyMs: 20, swap: true}, check: "agreement"},
		"scaling bug":     {m: model{w: 3, b: -4, scale: 1.8, latencyMs: 20}, check: "agreement"},
		"stale model":     {m: stale, check: "score-shape"},
		"NaN outputs":     {m: model{w: 3, b: -4, scale: 1, latencyMs: 20, nanRate: 0.02}, wantHard: true, check: "valid"},
		"2x latency":      {m: model{w: 3, b: -4, scale: 1, latencyMs: 40}, check: "latency", split: true},
		"crashing server": {m: model{w: 3, b: -4, scale: 1, latencyMs: 20, errRate: 0.08}, wantHard: true, check: "errors"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := newSim(7)
			phase := PhaseShadow
			if tc.split {
				phase = PhaseCanary
				s.split(40000, 0.25, good, tc.m)
			} else {
				s.shadow(12000, 0.2, good, tc.m)
			}
			v := s.verdict(phase, 3)
			if v.Decision != Fail {
				t.Fatalf("decision %s: %s", v.Decision, v.Reason)
			}
			if v.HardFailure != tc.wantHard {
				t.Errorf("hard=%v want %v (%s)", v.HardFailure, tc.wantHard, v.Reason)
			}
			found := false
			for _, c := range v.Checks {
				if c.Name == tc.check && c.Status == Fail {
					found = true
				}
			}
			if !found {
				t.Errorf("expected %s to fail; reason: %s", tc.check, v.Reason)
			}
		})
	}
}

func TestLatencySkippedInShadow(t *testing.T) {
	s := newSim(11)
	slow := good
	slow.latencyMs = 40
	s.shadow(12000, 0.2, good, slow)
	for _, c := range s.verdict(PhaseShadow, 1).Checks {
		if c.Type == CheckP99Ratio && c.Status != Skipped {
			t.Errorf("p99Ratio judged on mirrored traffic: %s", c.Message)
		}
	}
}

func TestHardFailureBeforeMinSamples(t *testing.T) {
	s := newSim(8)
	bad := good
	bad.nanRate = 0.5
	s.split(400, 0.5, good, bad)
	v := s.verdict(PhaseCanary, 1)
	if v.Decision != Fail || !v.HardFailure {
		t.Fatalf("decision %s hard=%v: %s", v.Decision, v.HardFailure, v.Reason)
	}
}

func TestDisagreementSamples(t *testing.T) {
	s := newSim(9)
	bad := good
	bad.swap = true
	s.shadow(500, 1, good, bad)
	v := s.verdict(PhaseShadow, 1)
	if len(v.Disagreements) == 0 {
		t.Fatal("no disagreement samples")
	}
	d := v.Disagreements[0]
	if *d.ByRev["stable"].Label == *d.ByRev["canary"].Label {
		t.Errorf("sample is not a disagreement: %+v", d)
	}
}

// A window must not include events from before it started, even when the
// start falls in the middle of a bucket.
func TestWindowExcludesEarlierEvents(t *testing.T) {
	s := newSim(12)
	s.shadow(500, 1, good, good) // 5s of shadow traffic, starting at a bucket boundary
	stepStart := s.t0.Add(5 * time.Second)
	if n := s.store.Track("ns/fraud", "canary", stepStart).Requests; n != 0 {
		t.Errorf("window starting mid-bucket includes %d earlier canary events", n)
	}
	if p := s.store.Pair("ns/fraud", "stable", "canary", stepStart); p.N != 0 {
		t.Errorf("window includes %d earlier pairs", p.N)
	}
	if n := s.store.Track("ns/fraud", "canary", s.t0).Requests; n == 0 {
		t.Error("window starting at the bucket boundary lost its events")
	}
}

func TestStoreMemoryBounded(t *testing.T) {
	s := newSim(10)
	s.shadow(200000, 0.2, good, good)
	st := s.store.Track("ns/fraud", "stable", s.t0)
	if n := len(st.Scores.Centroids()); n > 300 {
		t.Errorf("merged digest has %d centroids", n)
	}
	s.now = s.now.Add(time.Minute)
	s.store.GC()
	r := s.store.rollouts["ns/fraud"]
	if len(r.pending) != 0 {
		t.Errorf("pending pairs not collected: %d", len(r.pending))
	}
}
