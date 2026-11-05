// Package analysis turns captured prediction events into per-revision sketches
// and evaluates quality checks that compare a canary revision against the
// stable one over the same time window.
package analysis

import (
	"sort"
	"sync"
	"time"

	"github.com/modelgate/modelgate/internal/stats"
)

// Event is one captured request/response pair, as emitted by the sidecar.
// Raw payloads are never part of an event; only extracted fields.
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
}

// IsError reports whether the request failed at the server or transport.
func (e *Event) IsError() bool { return e.Status == 0 || e.Status >= 500 }

const (
	maxLabels     = 256 // distinct labels kept per bucket; the rest fold into otherLabel
	otherLabel    = "__other__"
	maxPending    = 200_000 // unpaired request IDs held for shadow pairing
	maxSamples    = 25      // disagreement samples kept per rollout
	maxIDsPerRoll = 2_000   // look counters kept per rollout
)

// TrackStats aggregates one revision's events within a time range. It is a
// sketch: memory is bounded regardless of traffic.
type TrackStats struct {
	Requests    int64            `json:"requests"`
	Errors      int64            `json:"errors"`
	Invalid     int64            `json:"invalid"`
	Predictions int64            `json:"predictions"` // successful, valid responses
	Labels      map[string]int64 `json:"labels,omitempty"`
	Scores      *stats.TDigest   `json:"-"`
	Latency     *stats.TDigest   `json:"-"`
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

type rolloutData struct {
	revs     map[string]map[int64]*TrackStats // revision -> bucket start -> stats
	pairs    map[pairKey]map[int64]*PairStats
	pending  map[string]pending // request ID -> first-seen event
	samples  []Disagreement     // ring buffer, newest last
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
	now       func() time.Time
}

// StoreOptions configures bucketing. Zero values pick defaults.
type StoreOptions struct {
	Bucket    time.Duration // default 15s
	Retention time.Duration // default 3h
	PairTTL   time.Duration // default 30s
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
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Store{rollouts: map[string]*rolloutData{}, bucket: o.Bucket, retention: o.Retention, pairTTL: o.PairTTL, now: o.Now}
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
			revs:    map[string]map[int64]*TrackStats{},
			pairs:   map[pairKey]map[int64]*PairStats{},
			pending: map[string]pending{},
			looks:   map[string]int{},
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
	}
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
