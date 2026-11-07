// Package analysis turns captured prediction events into per-revision sketches
// and evaluates quality checks that compare a canary revision against the
// stable one over the same time window.
package analysis

import (
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/modelgate/modelgate/internal/stats"
)

// Event is one captured request/response pair, as emitted by the sidecar.
// Raw payloads are part of an event only while payload capture is on.
type Event struct {
	Rollout   string    `json:"rollout"`  // "namespace/name"
	Revision  string    `json:"revision"` // pod template hash of the serving version
	RequestID string    `json:"requestId,omitempty"`
	Time      time.Time `json:"ts"`
	LatencyMs float64   `json:"latencyMs"`
	Status    int       `json:"status"`
	Label     *string   `json:"label,omitempty"`
	Score     *float64  `json:"score,omitempty"`
	// Invalid is a non-empty reason when the response was a success but its
	// prediction could not be used: unparseable JSON, NaN, missing field,
	// or a value outside the declared range.
	Invalid string `json:"invalid,omitempty"`
	// Payload holds redacted bodies while payload capture is on.
	Payload *Payload `json:"payload,omitempty"`
}

// Payload is a redacted copy of a request and its response. Bodies that
// could not be redacted are left out, and Omitted says why.
type Payload struct {
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Omitted  []string        `json:"omitted,omitempty"`
}

// PayloadRecord is a captured payload as the analyzer keeps it.
type PayloadRecord struct {
	RequestID string    `json:"requestId"`
	Revision  string    `json:"revision"`
	Time      time.Time `json:"ts"`
	Status    int       `json:"status"`
	Label     *string   `json:"label,omitempty"`
	Score     *float64  `json:"score,omitempty"`
	Payload
}

// IsError reports whether the request failed at the server or transport.
func (e *Event) IsError() bool { return e.Status == 0 || e.Status >= 500 }

const (
	maxLabels     = 256 // distinct labels kept per bucket; the rest fold into otherLabel
	otherLabel    = "__other__"
	maxPending    = 200_000 // unpaired request IDs held for shadow pairing
	maxSamples    = 25      // disagreement samples kept per rollout
	maxIDsPerRoll = 2_000   // look counters kept per rollout
	maxAwaiting   = 200_000 // predictions held per rollout while waiting for a label
	maxPayloads   = 500     // payloads kept per rollout, newest first out
	payloadTTL    = time.Hour
)

// Outcome is a ground-truth label that arrives after the prediction was
// served, for example a chargeback confirming fraud. It is joined with the
// captured predictions by request ID.
type Outcome struct {
	Rollout   string `json:"rollout"` // "namespace/name"
	RequestID string `json:"requestId"`
	Label     string `json:"label"`
}

// TrackStats aggregates one revision's events within a time range. It is a
// sketch: memory is bounded regardless of traffic.
type TrackStats struct {
	Requests    int64            `json:"requests"`
	Errors      int64            `json:"errors"`
	Invalid     int64            `json:"invalid"`
	Predictions int64            `json:"predictions"` // successful, valid responses
	Labels      map[string]int64 `json:"labels,omitempty"`
	// Labeled predictions whose ground truth has arrived, and how many of
	// them were correct. Counted in the bucket the prediction was served in.
	Labeled int64          `json:"labeled"`
	Correct int64          `json:"correct"`
	Scores  *stats.TDigest `json:"-"`
	Latency *stats.TDigest `json:"-"`
}

func newTrackStats() *TrackStats {
	return &TrackStats{
		Labels:  map[string]int64{},
		Scores:  stats.NewTDigest(0),
		Latency: stats.NewTDigest(0),
	}
}

func (s *TrackStats) add(e *Event) {
	s.Requests++
	s.Latency.Add(e.LatencyMs)
	switch {
	case e.IsError():
		s.Errors++
	case e.Invalid != "":
		s.Invalid++
	default:
		s.Predictions++
		if e.Label != nil {
			s.addLabel(*e.Label, 1)
		}
		if e.Score != nil {
			s.Scores.Add(*e.Score)
		}
	}
}

func (s *TrackStats) addLabel(l string, n int64) {
	if _, ok := s.Labels[l]; !ok && len(s.Labels) >= maxLabels {
		l = otherLabel
	}
	s.Labels[l] += n
}

func (s *TrackStats) merge(o *TrackStats) {
	s.Requests += o.Requests
	s.Errors += o.Errors
	s.Invalid += o.Invalid
	s.Predictions += o.Predictions
	s.Labeled += o.Labeled
	s.Correct += o.Correct
	for l, n := range o.Labels {
		s.addLabel(l, n)
	}
	s.Scores.Merge(o.Scores)
	s.Latency.Merge(o.Latency)
}

// PairStats aggregates shadow-mode comparisons of the same request served by
// two revisions. Diffs are oriented as (second revision - first revision)
// where the order is the one the pair key was created with.
type PairStats struct {
	N         int64   `json:"n"`         // paired requests with a label on both sides
	Agree     int64   `json:"agree"`     // of N, how many labels matched
	ScoredN   int64   `json:"scoredN"`   // paired requests with a score on both sides
	SumAbs    float64 `json:"sumAbs"`    // sum of |diff|
	SumAbsSq  float64 `json:"sumAbsSq"`  // sum of diff^2
	SumSigned float64 `json:"sumSigned"` // sum of diff
}

func (p *PairStats) merge(o *PairStats) {
	p.N += o.N
	p.Agree += o.Agree
	p.ScoredN += o.ScoredN
	p.SumAbs += o.SumAbs
	p.SumAbsSq += o.SumAbsSq
	p.SumSigned += o.SumSigned
}

// Prediction is one side of a disagreement sample.
type Prediction struct {
	Label *string  `json:"label,omitempty"`
	Score *float64 `json:"score,omitempty"`
}

// Disagreement records a request on which two revisions disagreed.
type Disagreement struct {
	RequestID string                `json:"requestId"`
	Time      time.Time             `json:"ts"`
	ByRev     map[string]Prediction `json:"byRevision"`
}

type pairKey struct{ a, b string } // a < b

type pending struct {
	ev   Event
	seen time.Time
}

// served is a classification awaiting its ground-truth label.
type served struct {
	revision string
	label    string
	at       time.Time
}

type rolloutData struct {
	awaiting map[string][]served              // request ID -> predictions made for it
	revs     map[string]map[int64]*TrackStats // revision -> bucket start -> stats
	pairs    map[pairKey]map[int64]*PairStats
	pending  map[string]pending // request ID -> first-seen event
	samples  []Disagreement     // ring buffer, newest last
	payloads []PayloadRecord    // ring buffer, newest last
	looks    map[string]int     // window key -> number of verdicts served
	lastSeen time.Time
}

// Store holds time-bucketed sketches for every active rollout.
type Store struct {
	mu        sync.Mutex
	rollouts  map[string]*rolloutData
	bucket    time.Duration
	retention time.Duration
	pairTTL   time.Duration
	labelTTL  time.Duration
	now       func() time.Time
}

// StoreOptions configures bucketing. Zero values pick defaults.
type StoreOptions struct {
	Bucket    time.Duration // default 15s
	Retention time.Duration // default 3h
	PairTTL   time.Duration // default 30s
	LabelTTL  time.Duration // how long a prediction waits for its label; default 1h
	Now       func() time.Time
}

// NewStore returns an empty store.
func NewStore(o StoreOptions) *Store {
	if o.Bucket <= 0 {
		o.Bucket = 15 * time.Second
	}
	if o.Retention <= 0 {
		o.Retention = 3 * time.Hour
	}
	if o.PairTTL <= 0 {
		o.PairTTL = 30 * time.Second
	}
	if o.LabelTTL <= 0 {
		o.LabelTTL = time.Hour
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Store{rollouts: map[string]*rolloutData{}, bucket: o.Bucket, retention: o.Retention, pairTTL: o.PairTTL,
		labelTTL: o.LabelTTL, now: o.Now}
}

// firstBucketFrom returns the first bucket that starts at or after since.
// Rounding up rather than down keeps a window from including events from
// before it began, such as mirrored shadow traffic leaking into step 1.
func (s *Store) firstBucketFrom(since time.Time) int64 {
	b := s.bucketOf(since)
	if time.Unix(b, 0).Before(since) {
		b += int64(s.bucket / time.Second)
	}
	return b
}

func (s *Store) bucketOf(t time.Time) int64 {
	return t.Truncate(s.bucket).Unix()
}

func (s *Store) rollout(name string) *rolloutData {
	r, ok := s.rollouts[name]
	if !ok {
		r = &rolloutData{
			awaiting: map[string][]served{},
			revs:     map[string]map[int64]*TrackStats{},
			pairs:    map[pairKey]map[int64]*PairStats{},
			pending:  map[string]pending{},
			looks:    map[string]int{},
		}
		s.rollouts[name] = r
	}
	return r
}

// Ingest adds events to the store.
func (s *Store) Ingest(events ...Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for i := range events {
		e := events[i]
		if e.Rollout == "" || e.Revision == "" {
			continue
		}
		if e.Time.IsZero() {
			e.Time = now
		}
		r := s.rollout(e.Rollout)
		r.lastSeen = now
		if e.Payload != nil {
			s.keepPayload(r, &e)
			e.Payload = nil // pairing and label buffers keep the event; not its bodies
		}
		b := s.bucketOf(e.Time)
		buckets, ok := r.revs[e.Revision]
		if !ok {
			buckets = map[int64]*TrackStats{}
			r.revs[e.Revision] = buckets
		}
		ts, ok := buckets[b]
		if !ok {
			ts = newTrackStats()
			buckets[b] = ts
		}
		ts.add(&e)
		s.pair(r, &e, now)
		s.await(r, &e)
	}
}

func (s *Store) keepPayload(r *rolloutData, e *Event) {
	if len(r.payloads) >= maxPayloads {
		r.payloads = append(r.payloads[:0], r.payloads[1:]...)
	}
	r.payloads = append(r.payloads, PayloadRecord{RequestID: e.RequestID, Revision: e.Revision, Time: e.Time,
		Status: e.Status, Label: e.Label, Score: e.Score, Payload: *e.Payload})
}

// Payloads returns a rollout's captured payloads, newest first. A non-empty
// requestID returns only that request's payloads, one per revision that
// served it. limit <= 0 means no limit.
func (s *Store) Payloads(rollout, requestID string, limit int) []PayloadRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rollouts[rollout]
	if !ok {
		return nil
	}
	cutoff := s.now().Add(-payloadTTL)
	out := []PayloadRecord{}
	for i := len(r.payloads) - 1; i >= 0; i-- {
		p := r.payloads[i]
		if p.Time.Before(cutoff) || (requestID != "" && p.RequestID != requestID) {
			continue
		}
		out = append(out, p)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// await keeps a classification until its label arrives or LabelTTL passes.
func (s *Store) await(r *rolloutData, e *Event) {
	if e.RequestID == "" || e.Label == nil || e.IsError() || e.Invalid != "" {
		return
	}
	prev, ok := r.awaiting[e.RequestID]
	if !ok && len(r.awaiting) >= maxAwaiting {
		return
	}
	for _, p := range prev {
		if p.revision == e.Revision {
			return // duplicate delivery
		}
	}
	r.awaiting[e.RequestID] = append(prev, served{revision: e.Revision, label: *e.Label, at: e.Time})
}

// IngestOutcomes joins ground-truth labels with the predictions made for the
// same requests, and returns how many predictions were scored. Labels for
// unknown or expired requests are ignored.
func (s *Store) IngestOutcomes(outcomes ...Outcome) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	scored := 0
	for _, o := range outcomes {
		r, ok := s.rollouts[o.Rollout]
		if !ok || o.RequestID == "" {
			continue
		}
		preds, ok := r.awaiting[o.RequestID]
		if !ok {
			continue
		}
		delete(r.awaiting, o.RequestID)
		for _, p := range preds {
			buckets, ok := r.revs[p.revision]
			if !ok {
				continue
			}
			ts, ok := buckets[s.bucketOf(p.at)]
			if !ok {
				continue // bucket already expired
			}
			ts.Labeled++
			if p.label == o.Label {
				ts.Correct++
			}
			scored++
		}
	}
	return scored
}

// pair matches a mirrored request with its original by request ID.
func (s *Store) pair(r *rolloutData, e *Event, now time.Time) {
	if e.RequestID == "" || e.IsError() || e.Invalid != "" {
		return
	}
	p, ok := r.pending[e.RequestID]
	if !ok {
		if len(r.pending) < maxPending {
			r.pending[e.RequestID] = pending{ev: *e, seen: now}
		}
		return
	}
	if p.ev.Revision == e.Revision {
		return // duplicate delivery from the same version
	}
	delete(r.pending, e.RequestID)
	first, second := &p.ev, e
	key := pairKey{first.Revision, second.Revision}
	if key.a > key.b {
		key = pairKey{key.b, key.a}
		first, second = second, first
	}
	buckets, ok := r.pairs[key]
	if !ok {
		buckets = map[int64]*PairStats{}
		r.pairs[key] = buckets
	}
	b := s.bucketOf(e.Time)
	ps, ok := buckets[b]
	if !ok {
		ps = &PairStats{}
		buckets[b] = ps
	}
	disagree := false
	if first.Label != nil && second.Label != nil {
		ps.N++
		if *first.Label == *second.Label {
			ps.Agree++
		} else {
			disagree = true
		}
	}
	if first.Score != nil && second.Score != nil {
		d := *second.Score - *first.Score
		ps.ScoredN++
		ps.SumSigned += d
		if d < 0 {
			d = -d
		}
		ps.SumAbs += d
		ps.SumAbsSq += d * d
	}
	if disagree {
		r.samples = append(r.samples, Disagreement{
			RequestID: e.RequestID,
			Time:      e.Time,
			ByRev: map[string]Prediction{
				first.Revision:  {Label: first.Label, Score: first.Score},
				second.Revision: {Label: second.Label, Score: second.Score},
			},
		})
		if len(r.samples) > maxSamples {
			r.samples = r.samples[len(r.samples)-maxSamples:]
		}
	}
}

// Track returns merged stats for a revision over [since, now].
func (s *Store) Track(rollout, revision string, since time.Time) *TrackStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := newTrackStats()
	r, ok := s.rollouts[rollout]
	if !ok {
		return out
	}
	from := s.firstBucketFrom(since)
	for b, ts := range r.revs[revision] {
		if b >= from {
			out.merge(ts)
		}
	}
	return out
}

// Pair returns shadow comparison stats over [since, now], oriented so that
// diffs are (canary - stable).
func (s *Store) Pair(rollout, stable, canary string, since time.Time) *PairStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := &PairStats{}
	r, ok := s.rollouts[rollout]
	if !ok {
		return out
	}
	key, flip := pairKey{stable, canary}, false
	if key.a > key.b {
		key, flip = pairKey{canary, stable}, true
	}
	from := s.firstBucketFrom(since)
	for b, ps := range r.pairs[key] {
		if b >= from {
			out.merge(ps)
		}
	}
	if flip {
		out.SumSigned = -out.SumSigned
	}
	return out
}

// Disagreements returns the most recent disagreement samples between two
// revisions since the given time, newest first.
func (s *Store) Disagreements(rollout, stable, canary string, since time.Time) []Disagreement {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.rollouts[rollout]
	if !ok {
		return nil
	}
	var out []Disagreement
	for i := len(r.samples) - 1; i >= 0; i-- {
		d := r.samples[i]
		_, hasS := d.ByRev[stable]
		_, hasC := d.ByRev[canary]
		if hasS && hasC && !d.Time.Before(since) {
			out = append(out, d)
		}
	}
	return out
}

// NextLook increments and returns the look counter for an analysis window.
// The look number drives alpha spending, so callers that cannot track it
// themselves (Argo Rollouts, Flagger) still get a valid false-alarm budget.
func (s *Store) NextLook(rollout, window string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rollout(rollout)
	if len(r.looks) >= maxIDsPerRoll {
		r.looks = map[string]int{}
	}
	r.looks[window]++
	return r.looks[window]
}

// GC drops expired buckets, stale pending pairs, and idle rollouts.
func (s *Store) GC() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	oldest := s.bucketOf(now.Add(-s.retention))
	for name, r := range s.rollouts {
		if now.Sub(r.lastSeen) > s.retention {
			delete(s.rollouts, name)
			continue
		}
		for rev, buckets := range r.revs {
			for b := range buckets {
				if b < oldest {
					delete(buckets, b)
				}
			}
			if len(buckets) == 0 {
				delete(r.revs, rev)
			}
		}
		for k, buckets := range r.pairs {
			for b := range buckets {
				if b < oldest {
					delete(buckets, b)
				}
			}
			if len(buckets) == 0 {
				delete(r.pairs, k)
			}
		}
		for id, p := range r.pending {
			if now.Sub(p.seen) > s.pairTTL {
				delete(r.pending, id)
			}
		}
		for id, preds := range r.awaiting {
			if now.Sub(preds[0].at) > s.labelTTL {
				delete(r.awaiting, id)
			}
		}
		keep := r.payloads[:0]
		for _, p := range r.payloads {
			if now.Sub(p.Time) <= payloadTTL {
				keep = append(keep, p)
			}
		}
		clear(r.payloads[len(keep):]) // release the dropped bodies
		r.payloads = keep
	}
}

// Rollouts lists rollouts that currently have data, for debugging and metrics.
func (s *Store) Rollouts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.rollouts))
	for k := range s.rollouts {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
