// Command benchmark runs the PRD's bad-model suite: every demo variant is
// rolled out many times against the good model, and each run reports
// whether it was promoted or rolled back, how fast, and how much live
// traffic the canary served before the decision.
//
// Predictions come from the real scikit-learn variants (exported by
// demo/model/export_pool.py) and verdicts from the real analysis engine.
// The controller's decision rules are mirrored here in simulated time so
// hundreds of 40-minute rollouts finish in seconds; the cluster e2e covers
// the real controller.
//
//	python demo/model/export_pool.py --out bench/pool.json
//	go run ./hack/benchmark -pool bench/pool.json -runs 50
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
)

type pool struct {
	Size     int      `json:"size"`
	Truth    []string `json:"truth"`
	Variants map[string]struct {
		Labels []string   `json:"labels"`
		Scores []*float64 `json:"scores"`
	} `json:"variants"`
}

type stage struct {
	shadow     bool
	weight     float64
	sampleRate float64
	pause      time.Duration
}

// The rollout from the PRD's FR1 example.
var (
	stages = []stage{
		{shadow: true, sampleRate: 0.2, pause: 10 * time.Minute},
		{weight: 0.05, pause: 10 * time.Minute},
		{weight: 0.25, pause: 15 * time.Minute},
		{weight: 0.50, pause: 15 * time.Minute},
	}
	checks = []analysis.CheckSpec{
		{Name: "agreement", Type: analysis.CheckAgreementRate, Min: f(0.97), Phase: analysis.PhaseShadow},
		{Name: "pred-shift", Type: analysis.CheckPSI, Max: f(0.10)},
		{Name: "score-shape", Type: analysis.CheckKS, Max: f(0.10)},
		{Name: "valid", Type: analysis.CheckValidity, Max: f(0)},
		{Name: "latency", Type: analysis.CheckP99Ratio, Max: f(1.20)},
		{Name: "errors", Type: analysis.CheckErrorRate, Max: f(0.01)},
	}
)

const (
	minSamples   = 2000
	failureLimit = 2
	interval     = 30 * time.Second
	maxStageTime = 2 * time.Hour // a stage held this long counts as stuck
)

func f(v float64) *float64 { return &v }

type outcome struct {
	promoted bool
	stuck    bool
	check    string        // failing check that triggered rollback
	stage    int           // stage index at decision
	elapsed  time.Duration // rollout start to decision
	exposure float64       // share of user requests served by the canary
}

// Delayed-label mode (-label-delay > 0): a labelAccuracy check is added,
// and a -label-rate share of requests receives its true class that long
// after being served. With -labels-only the label-free quality checks are
// dropped, which shows how long ground truth alone takes.
var (
	labelDelay time.Duration
	labelRate  float64
)

func main() {
	poolPath := flag.String("pool", "bench/pool.json", "prediction pool from export_pool.py")
	runs := flag.Int("runs", 50, "runs per variant")
	rps := flag.Float64("rps", 50, "user requests per second")
	only := flag.String("variants", "identical,swapped,scaling,stale,accuracy-drop,nan,slow", "comma-separated variants")
	flag.DurationVar(&labelDelay, "label-delay", 0, "delay before ground-truth labels arrive; 0 disables labels")
	flag.Float64Var(&labelRate, "label-rate", 0.5, "share of requests that get a ground-truth label")
	labelsOnly := flag.Bool("labels-only", false, "drop the label-free quality checks (needs -label-delay)")
	flag.Parse()
	if labelDelay > 0 {
		if *labelsOnly {
			checks = []analysis.CheckSpec{checks[3], checks[4], checks[5]} // validity, latency, errors
		}
		checks = append(checks, analysis.CheckSpec{Name: "accuracy", Type: analysis.CheckLabelAccuracy, Max: f(0.02)})
	}

	raw, err := os.ReadFile(*poolPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	var p pool
	if err := json.Unmarshal(raw, &p); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("ModelGate bad-model suite: %d runs per variant, %.0f req/s, minSamples %d, PRD stages (shadow 10m@20%%, 5%%/10m, 25%%/15m, 50%%/15m)\n", *runs, *rps, minSamples)
	if labelDelay > 0 {
		if len(p.Truth) != p.Size {
			fmt.Fprintln(os.Stderr, "pool has no ground truth; re-export it with demo/model/export_pool.py")
			os.Exit(1)
		}
		fmt.Printf("Delayed labels: %.0f%% of requests labeled after %s; labels-only=%v\n", labelRate*100, labelDelay, *labelsOnly)
	}
	fmt.Println()
	fmt.Println("| Variant | Expected | Caught | False rollback | Median time to decision | Median exposure | Caught by | Stage of decision |")
	fmt.Println("| --- | --- | --- | --- | --- | --- | --- | --- |")
	for _, v := range strings.Split(*only, ",") {
		if _, ok := p.Variants[v]; !ok {
			fmt.Fprintf(os.Stderr, "variant %q not in pool\n", v)
			os.Exit(1)
		}
		var res []outcome
		for i := 0; i < *runs; i++ {
			res = append(res, simulate(&p, v, *rps, int64(i+1)))
		}
		report(v, res)
	}
}

func report(variant string, res []outcome) {
	bad := variant != "identical"
	var rolled, promoted, stuck int
	var times, exps []float64
	byCheck, byStage := map[string]int{}, map[int]int{}
	for _, r := range res {
		switch {
		case r.stuck:
			stuck++
		case r.promoted:
			promoted++
		default:
			rolled++
			byCheck[r.check]++
		}
		byStage[r.stage]++
		times = append(times, r.elapsed.Minutes())
		exps = append(exps, r.exposure)
	}
	n := float64(len(res))
	expected, caught, falseRB := "promote", "-", fmt.Sprintf("%.0f%% (%d/%d)", 100*float64(rolled)/n, rolled, len(res))
	if bad {
		expected, caught, falseRB = "rollback", fmt.Sprintf("%.0f%% (%d/%d)", 100*float64(rolled)/n, rolled, len(res)), "-"
	}
	if stuck > 0 {
		caught += fmt.Sprintf(", %d stuck", stuck)
	}
	fmt.Printf("| %s | %s | %s | %s | %.1f min | %.2f%% | %s | %s |\n",
		variant, expected, caught, falseRB, median(times), 100*median(exps), counts(byCheck), stageCounts(byStage))
}

func counts(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s %d", k, v))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

func stageCounts(m map[int]int) string {
	names := []string{"shadow", "5%", "25%", "50%", "done"}
	var parts []string
	for i, name := range names {
		if m[i] > 0 {
			parts = append(parts, fmt.Sprintf("%s %d", name, m[i]))
		}
	}
	return strings.Join(parts, ", ")
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[len(s)/2]
}

// latency models a server with fixed overhead plus lognormal model time.
func latency(r *rand.Rand, slow bool) float64 {
	model := 20.0
	if slow {
		model = 40
	}
	return 8 + model*math.Exp(0.25*r.NormFloat64())
}

func simulate(p *pool, variant string, rps float64, seed int64) outcome {
	r := rand.New(rand.NewSource(seed))
	t0 := time.Date(2026, 11, 5, 0, 0, 0, 0, time.UTC)
	now := t0
	store := analysis.NewStore(analysis.StoreOptions{Now: func() time.Time { return now }})
	eng := &analysis.Engine{Store: store}
	good, canary := p.Variants["good"], p.Variants[variant]
	slow := variant == "slow"

	type due struct {
		at time.Time
		o  analysis.Outcome
	}
	var pendingLabels []due
	var userReqs, canaryUser float64
	seq := 0
	event := func(rev string, id string, i int, labels []string, scores []*float64, isSlow bool) analysis.Event {
		e := analysis.Event{Rollout: "bench/m", Revision: rev, RequestID: id, Time: now, Status: 200, LatencyMs: latency(r, isSlow)}
		if scores[i] == nil {
			e.Invalid = "score is NaN or infinite"
			return e
		}
		e.Label, e.Score = &labels[i], scores[i]
		return e
	}
	done := func(o outcome) outcome {
		o.elapsed = now.Sub(t0)
		if userReqs > 0 {
			o.exposure = canaryUser / userReqs
		}
		return o
	}

	for si, st := range stages {
		start := now
		look, failures := 0, 0
		nextLook := now.Add(interval)
		for {
			// One simulated second of traffic.
			var batch []analysis.Event
			n := int(rps)
			if r.Float64() < rps-float64(n) {
				n++
			}
			for k := 0; k < n; k++ {
				seq++
				id := fmt.Sprint(seq)
				i := r.Intn(p.Size)
				userReqs++
				if labelDelay > 0 && r.Float64() < labelRate {
					pendingLabels = append(pendingLabels, due{now.Add(labelDelay), analysis.Outcome{Rollout: "bench/m", RequestID: id, Label: p.Truth[i]}})
				}
				switch {
				case st.shadow:
					batch = append(batch, event("stable", id, i, good.Labels, good.Scores, false))
					if r.Float64() < st.sampleRate {
						batch = append(batch, event("canary", id, i, canary.Labels, canary.Scores, slow))
					}
				case r.Float64() < st.weight:
					canaryUser++
					batch = append(batch, event("canary", id, i, canary.Labels, canary.Scores, slow))
				default:
					batch = append(batch, event("stable", id, i, good.Labels, good.Scores, false))
				}
			}
			store.Ingest(batch...)
			k := 0
			for k < len(pendingLabels) && !pendingLabels[k].at.After(now) {
				store.IngestOutcomes(pendingLabels[k].o)
				k++
			}
			pendingLabels = pendingLabels[k:]
			now = now.Add(time.Second)
			if now.Second() == 0 {
				store.GC() // as the analyzer does every minute
			}
			if now.Before(nextLook) {
				continue
			}
			nextLook = now.Add(interval)

			phase := analysis.PhaseCanary
			if st.shadow {
				phase = analysis.PhaseShadow
			}
			look++
			v := eng.Evaluate(analysis.Request{
				Rollout: "bench/m", Stable: "stable", Canary: "canary", Since: start, RolloutStart: t0,
				Phase: phase, Look: look, MinSamples: minSamples, Alpha: analysis.DefaultAlpha, Checks: checks,
			})
			switch v.Decision {
			case analysis.Fail:
				failures++
				if v.HardFailure || failures >= failureLimit {
					return done(outcome{check: firstFailing(v), stage: si})
				}
				continue
			case analysis.Inconclusive:
				if now.Sub(start) > maxStageTime {
					return done(outcome{stuck: true, stage: si})
				}
				continue
			}
			failures = 0
			if now.Sub(start) >= st.pause {
				break
			}
		}
	}
	return done(outcome{promoted: true, stage: len(stages)})
}

func firstFailing(v analysis.Verdict) string {
	for _, c := range v.Checks {
		if c.Status == analysis.Fail && c.Hard {
			return c.Name
		}
	}
	for _, c := range v.Checks {
		if c.Status == analysis.Fail {
			return c.Name
		}
	}
	return "?"
}
