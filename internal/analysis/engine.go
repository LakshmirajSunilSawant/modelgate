package analysis

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/modelgate/modelgate/internal/stats"
)

// Check types.
const (
	CheckAgreementRate = "agreementRate" // shadow, classification: share of identical decisions
	CheckMeanAbsDiff   = "meanAbsDiff"   // shadow, regression/score: mean |canary - stable|
	CheckPSI           = "psi"           // predicted class mix (or score deciles) shift
	CheckChiSquare     = "chiSquare"     // predicted class mix shift, Cramér's V effect size
	CheckKS            = "ks"            // score distribution shape
	CheckWasserstein   = "wasserstein"   // score distribution distance
	CheckValidity      = "validity"      // share of invalid outputs (NaN, out of range, schema)
	CheckP99Ratio      = "p99Ratio"      // canary p99 latency / stable p99 latency
	CheckErrorRate     = "errorRate"     // canary error rate
)

// Phases.
const (
	PhaseShadow = "shadow"
	PhaseCanary = "canary"
)

// Check results and verdict decisions.
const (
	Pass         = "pass"
	Fail         = "fail"
	Inconclusive = "inconclusive"
	Skipped      = "skipped"
)

// CheckSpec configures one check. Exactly one of Min or Max is the threshold,
// depending on the check type.
type CheckSpec struct {
	Name  string   `json:"name"`
	Type  string   `json:"type"`
	Min   *float64 `json:"min,omitempty"`
	Max   *float64 `json:"max,omitempty"`
	Phase string   `json:"phase,omitempty"` // shadow | canary | "" for both
}

// Request asks for a verdict on a canary revision against the stable one.
type Request struct {
	Rollout    string      `json:"rollout"` // "namespace/name"
	Stable     string      `json:"stable"`
	Canary     string      `json:"canary"`
	Since      time.Time   `json:"since"`
	Phase      string      `json:"phase"`
	Look       int         `json:"look,omitempty"` // 0: analyzer counts looks itself
	MinSamples int64       `json:"minSamples"`
	Alpha      float64     `json:"alpha,omitempty"` // false-alarm budget per check per stage
	Checks     []CheckSpec `json:"checks"`
}

// CheckResult is the outcome of one check.
type CheckResult struct {
	Name      string  `json:"name"`
	Type      string  `json:"type"`
	Status    string  `json:"status"`
	Hard      bool    `json:"hard,omitempty"`
	Value     float64 `json:"value"`
	Bound     float64 `json:"bound"` // the confidence bound compared against the threshold
	Threshold float64 `json:"threshold"`
	Samples   int64   `json:"samples"`
	Message   string  `json:"message"`
}

// Verdict is the analyzer's answer for one look.
type Verdict struct {
	Decision      string         `json:"decision"`
	HardFailure   bool           `json:"hardFailure"`
	Reason        string         `json:"reason"`
	Look          int            `json:"look"`
	StableSamples int64          `json:"stableSamples"`
	CanarySamples int64          `json:"canarySamples"`
	PairedSamples int64          `json:"pairedSamples"`
	Checks        []CheckResult  `json:"checks"`
	Disagreements []Disagreement `json:"disagreements,omitempty"`
}

// DefaultAlpha is the per-check, per-stage false-alarm budget.
const DefaultAlpha = 0.01

// Engine evaluates checks against a store.
type Engine struct {
	Store *Store
}

// Evaluate computes a verdict. Decision rules:
//   - Hard checks (validity, errorRate) fail as soon as the evidence is
//     conclusive, even before minSamples.
//   - Other checks report inconclusive until minSamples is reached for the
//     samples they use.
//   - A check fails only when its confidence bound, not just its point
//     estimate, crosses the threshold. Every bound uses alpha spending over
//     looks, so polling does not inflate false alarms.
//   - The verdict fails if any check fails, is inconclusive if any check is,
//     and passes otherwise.
func (e *Engine) Evaluate(req Request) Verdict {
	if req.Alpha <= 0 {
		req.Alpha = DefaultAlpha
	}
	if req.Phase == "" {
		req.Phase = PhaseCanary
	}
	look := req.Look
	if look <= 0 {
		look = e.Store.NextLook(req.Rollout, fmt.Sprintf("%s|%s|%s|%d", req.Stable, req.Canary, req.Phase, req.Since.Unix()))
	}
	st := e.Store.Track(req.Rollout, req.Stable, req.Since)
	ca := e.Store.Track(req.Rollout, req.Canary, req.Since)
	pr := e.Store.Pair(req.Rollout, req.Stable, req.Canary, req.Since)

	c := checkCtx{req: req, look: look, alphaLook: stats.LookAlpha(req.Alpha, look), stable: st, canary: ca, pair: pr}
	v := Verdict{Look: look, StableSamples: st.Requests, CanarySamples: ca.Requests, PairedSamples: max(pr.N, pr.ScoredN)}
	for _, spec := range req.Checks {
		v.Checks = append(v.Checks, c.run(spec))
	}

	inconclusive := 0
	var failed []CheckResult
	for _, r := range v.Checks {
		switch r.Status {
		case Fail:
			failed = append(failed, r)
		case Inconclusive:
			inconclusive++
		}
	}
	switch {
	case len(failed) > 0:
		// Hard failures first, so the reason names the most urgent check.
		sort.SliceStable(failed, func(i, j int) bool { return failed[i].Hard && !failed[j].Hard })
		v.Decision = Fail
		v.HardFailure = failed[0].Hard
		v.Reason = failed[0].Message
		if len(failed) > 1 {
			v.Reason += fmt.Sprintf(" (+%d more failing)", len(failed)-1)
		}
	case inconclusive > 0:
		v.Decision = Inconclusive
		v.Reason = fmt.Sprintf("%d check(s) waiting for evidence", inconclusive)
	default:
		v.Decision = Pass
		v.Reason = "all checks passed"
	}
	if req.Phase == PhaseShadow {
		v.Disagreements = e.Store.Disagreements(req.Rollout, req.Stable, req.Canary, req.Since)
		if len(v.Disagreements) > 10 {
			v.Disagreements = v.Disagreements[:10]
		}
	}
	return v
}

type checkCtx struct {
	req            Request
	look           int
	alphaLook      float64
	stable, canary *TrackStats
	pair           *PairStats
}

func (c *checkCtx) run(s CheckSpec) CheckResult {
	r := CheckResult{Name: s.Name, Type: s.Type}
	if s.Phase != "" && s.Phase != c.req.Phase {
		r.Status = Skipped
		r.Message = fmt.Sprintf("%s: only evaluated in %s phase", s.Name, s.Phase)
		return r
	}
	// Mirrored shadow traffic is open-loop (the gateway does not wait for the
	// canary) while stable traffic is closed-loop, so near capacity the canary
	// queues more and its latency is inflated even for identical code. Latency
	// is therefore judged only on split traffic unless asked for explicitly.
	if s.Type == CheckP99Ratio && s.Phase == "" && c.req.Phase == PhaseShadow {
		r.Status = Skipped
		r.Message = fmt.Sprintf("%s: latency is not comparable under mirrored traffic; judged in canary steps", s.Name)
		return r
	}
	switch s.Type {
	case CheckAgreementRate:
		c.agreement(s, &r)
	case CheckMeanAbsDiff:
		c.meanAbsDiff(s, &r)
	case CheckPSI:
		c.psi(s, &r)
	case CheckChiSquare:
		c.chiSquare(s, &r)
	case CheckKS:
		c.ks(s, &r)
	case CheckWasserstein:
		c.wasserstein(s, &r)
	case CheckValidity:
		c.validity(s, &r)
	case CheckErrorRate:
		c.errorRate(s, &r)
	case CheckP99Ratio:
		c.p99Ratio(s, &r)
	default:
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: unknown check type %q", s.Name, s.Type)
	}
	return r
}

func threshold(p *float64, def float64) float64 {
	if p != nil {
		return *p
	}
	return def
}

func (c *checkCtx) enough(r *CheckResult, n int64) bool {
	r.Samples = n
	if n < c.req.MinSamples {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: %d/%d samples", r.Name, n, c.req.MinSamples)
		return false
	}
	return true
}

func (c *checkCtx) comparativeN() int64 {
	return min(c.stable.Predictions, c.canary.Predictions)
}

// agreementRate fails when the upper confidence bound of the agreement rate
// is below min: the canary confidently flips more decisions than allowed.
func (c *checkCtx) agreement(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Min, 0.97)
	if c.req.Phase != PhaseShadow {
		r.Status = Skipped
		r.Message = fmt.Sprintf("%s: needs shadow traffic", s.Name)
		return
	}
	if !c.enough(r, c.pair.N) {
		return
	}
	m := ratio(c.pair.Agree, c.pair.N)
	_, hi := stats.BernoulliBounds(c.pair.Agree, c.pair.N, c.alphaLook)
	r.Value, r.Bound = m, hi
	if r.Bound < r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: agreement %.4f (upper bound %.4f) below min %.4f", s.Name, m, r.Bound, r.Threshold)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: agreement %.4f", s.Name, m)
}

// meanAbsDiff fails when the lower confidence bound of mean |canary-stable|
// exceeds max. Scores are not assumed bounded, so this uses a normal
// approximation with alpha spending.
func (c *checkCtx) meanAbsDiff(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 0.05)
	if c.req.Phase != PhaseShadow {
		r.Status = Skipped
		r.Message = fmt.Sprintf("%s: needs shadow traffic", s.Name)
		return
	}
	if !c.enough(r, c.pair.ScoredN) {
		return
	}
	n := float64(c.pair.ScoredN)
	mean := c.pair.SumAbs / n
	variance := math.Max(0, c.pair.SumAbsSq/n-mean*mean)
	z := stats.NormalQuantile(1 - c.alphaLook)
	r.Value = mean
	r.Bound = mean - z*math.Sqrt(variance/n)
	if r.Bound > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: mean abs diff %.4g (lower bound %.4g) above max %.4g; mean signed diff %+.4g", s.Name, mean, r.Bound, r.Threshold, c.pair.SumSigned/n)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: mean abs diff %.4g", s.Name, mean)
}

// categories returns aligned count vectors for stable and canary: label
// counts for classification, or score counts binned on stable deciles.
func (c *checkCtx) categories() (st, ca []float64, kind string) {
	if len(c.stable.Labels) > 0 || len(c.canary.Labels) > 0 {
		keys := map[string]struct{}{}
		for k := range c.stable.Labels {
			keys[k] = struct{}{}
		}
		for k := range c.canary.Labels {
			keys[k] = struct{}{}
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			st = append(st, float64(c.stable.Labels[k]))
			ca = append(ca, float64(c.canary.Labels[k]))
		}
		return st, ca, "class"
	}
	if c.stable.Scores.Count() == 0 || c.canary.Scores.Count() == 0 {
		return nil, nil, ""
	}
	// Decile edges of the stable distribution; each stable bin holds ~10%.
	const bins = 10
	edges := make([]float64, 0, bins-1)
	for i := 1; i < bins; i++ {
		edges = append(edges, c.stable.Scores.Quantile(float64(i)/bins))
	}
	binCounts := func(d *stats.TDigest) []float64 {
		out := make([]float64, bins)
		n := d.Count()
		prev := 0.0
		for i, e := range edges {
			cdf := d.CDF(e)
			out[i] = (cdf - prev) * n
			prev = cdf
		}
		out[bins-1] = (1 - prev) * n
		return out
	}
	return binCounts(c.stable.Scores), binCounts(c.canary.Scores), "score decile"
}

// psi fails when PSI exceeds max and a chi-square test (alpha-spent) confirms
// the shift is not sampling noise.
func (c *checkCtx) psi(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 0.1)
	if !c.enough(r, c.comparativeN()) {
		return
	}
	st, ca, kind := c.categories()
	if kind == "" {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no labels or scores captured", s.Name)
		return
	}
	r.Value = stats.PSI(st, ca)
	_, _, p, _ := stats.ContingencyChiSquare(st, ca)
	r.Bound = p
	if r.Value > r.Threshold && p < c.alphaLook {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: %s PSI %.4f above max %.4f (chi-square p=%.2g)%s", s.Name, kind, r.Value, r.Threshold, p, topShift(c, kind))
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: %s PSI %.4f", s.Name, kind, r.Value)
}

// topShift names the class whose share moved the most, for the failure message.
func topShift(c *checkCtx, kind string) string {
	if kind != "class" {
		return ""
	}
	sn, cn := float64(c.stable.Predictions), float64(c.canary.Predictions)
	if sn == 0 || cn == 0 {
		return ""
	}
	best, bestD := "", 0.0
	for l := range mergeKeys(c.stable.Labels, c.canary.Labels) {
		d := math.Abs(float64(c.canary.Labels[l])/cn - float64(c.stable.Labels[l])/sn)
		if d > bestD || (d == bestD && l < best) {
			best, bestD = l, d
		}
	}
	return fmt.Sprintf("; class %q share %.1f%% -> %.1f%%", best, 100*float64(c.stable.Labels[best])/sn, 100*float64(c.canary.Labels[best])/cn)
}

func mergeKeys(a, b map[string]int64) map[string]struct{} {
	out := map[string]struct{}{}
	for k := range a {
		out[k] = struct{}{}
	}
	for k := range b {
		out[k] = struct{}{}
	}
	return out
}

// chiSquare fails when the class mix differs significantly (alpha-spent) and
// Cramér's V exceeds max, so huge samples do not fail on trivial shifts.
func (c *checkCtx) chiSquare(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 0.1)
	if !c.enough(r, c.comparativeN()) {
		return
	}
	st, ca, kind := c.categories()
	if kind == "" {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no labels or scores captured", s.Name)
		return
	}
	stat, df, p, v := stats.ContingencyChiSquare(st, ca)
	r.Value, r.Bound = v, p
	if p < c.alphaLook && v > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: %s mix shifted, Cramér's V %.4f above max %.4f (chi2=%.1f df=%d p=%.2g)", s.Name, kind, v, r.Threshold, stat, df, p)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: Cramér's V %.4f (p=%.2g)", s.Name, v, p)
}

// ks fails when the KS statistic minus both DKW radii exceeds max: even after
// allowing for sampling error in both CDFs, the distributions differ by more
// than the threshold.
func (c *checkCtx) ks(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 0.1)
	if !c.enough(r, c.comparativeN()) {
		return
	}
	if c.stable.Scores.Count() == 0 || c.canary.Scores.Count() == 0 {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no scores captured", s.Name)
		return
	}
	d := stats.KS(c.stable.Scores, c.canary.Scores)
	eps := stats.DKW(c.stable.Scores.Count(), c.alphaLook/2) + stats.DKW(c.canary.Scores.Count(), c.alphaLook/2)
	r.Value, r.Bound = d, d-eps
	if r.Bound > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: KS %.4f (lower bound %.4f) above max %.4f", s.Name, d, r.Bound, r.Threshold)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: KS %.4f", s.Name, d)
}

// wasserstein fails when the distance exceeds max and the KS test rejects
// equality of the two distributions at the spent alpha.
func (c *checkCtx) wasserstein(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 0.05)
	if !c.enough(r, c.comparativeN()) {
		return
	}
	if c.stable.Scores.Count() == 0 || c.canary.Scores.Count() == 0 {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no scores captured", s.Name)
		return
	}
	w := stats.Wasserstein(c.stable.Scores, c.canary.Scores)
	d := stats.KS(c.stable.Scores, c.canary.Scores)
	eps := stats.DKW(c.stable.Scores.Count(), c.alphaLook/2) + stats.DKW(c.canary.Scores.Count(), c.alphaLook/2)
	r.Value, r.Bound = w, d-eps
	if w > r.Threshold && d > eps {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: Wasserstein %.4g above max %.4g (KS %.4f > noise %.4f)", s.Name, w, r.Threshold, d, eps)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: Wasserstein %.4g", s.Name, w)
}

// validity is a hard check on the share of canary responses whose
// prediction is unusable. With max 0 (the default) any invalid output fails.
func (c *checkCtx) validity(s CheckSpec, r *CheckResult) {
	r.Hard = true
	r.Threshold = threshold(s.Max, 0)
	n := c.canary.Predictions + c.canary.Invalid
	r.Samples = n
	if n == 0 {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no canary responses yet", s.Name)
		return
	}
	m := ratio(c.canary.Invalid, n)
	r.Value = m
	r.Bound, _ = stats.BernoulliBounds(c.canary.Invalid, n, c.alphaLook)
	if (r.Threshold == 0 && c.canary.Invalid > 0) || r.Bound > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: %d of %d canary outputs invalid (%.2f%%), max %.2f%%", s.Name, c.canary.Invalid, n, 100*m, 100*r.Threshold)
		return
	}
	if !c.enough(r, n) {
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: %.3f%% invalid", s.Name, 100*m)
}

// errorRate is a hard check: it fails as soon as the lower confidence bound
// of the canary error rate exceeds max.
func (c *checkCtx) errorRate(s CheckSpec, r *CheckResult) {
	r.Hard = true
	r.Threshold = threshold(s.Max, 0.01)
	n := c.canary.Requests
	r.Samples = n
	if n == 0 {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: no canary requests yet", s.Name)
		return
	}
	m := ratio(c.canary.Errors, n)
	r.Value = m
	r.Bound, _ = stats.BernoulliBounds(c.canary.Errors, n, c.alphaLook)
	if r.Bound > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: canary error rate %.2f%% (lower bound %.2f%%) above max %.2f%%; stable %.2f%%",
			s.Name, 100*m, 100*r.Bound, 100*r.Threshold, 100*ratio(c.stable.Errors, c.stable.Requests))
		return
	}
	if !c.enough(r, n) {
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: error rate %.3f%%", s.Name, 100*m)
}

func ratio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// p99Ratio fails when a conservative ratio, the canary's lower-band p99 over
// the stable's upper-band p99, exceeds max.
func (c *checkCtx) p99Ratio(s CheckSpec, r *CheckResult) {
	r.Threshold = threshold(s.Max, 1.2)
	n := min(c.stable.Requests, c.canary.Requests)
	if !c.enough(r, n) {
		return
	}
	const q = 0.99
	sn, cn := c.stable.Latency.Count(), c.canary.Latency.Count()
	_, sHi := stats.QuantileBand(q, sn, c.alphaLook)
	cLo, _ := stats.QuantileBand(q, cn, c.alphaLook)
	stableP99 := c.stable.Latency.Quantile(q)
	canaryP99 := c.canary.Latency.Quantile(q)
	if stableP99 <= 0 {
		r.Status = Inconclusive
		r.Message = fmt.Sprintf("%s: stable p99 is zero", s.Name)
		return
	}
	r.Value = canaryP99 / stableP99
	r.Bound = c.canary.Latency.Quantile(cLo) / c.stable.Latency.Quantile(sHi)
	if r.Bound > r.Threshold {
		r.Status = Fail
		r.Message = fmt.Sprintf("%s: p99 %.1fms vs stable %.1fms, ratio %.2f (conservative %.2f) above max %.2f", s.Name, canaryP99, stableP99, r.Value, r.Bound, r.Threshold)
		return
	}
	r.Status = Pass
	r.Message = fmt.Sprintf("%s: p99 ratio %.2f", s.Name, r.Value)
}
