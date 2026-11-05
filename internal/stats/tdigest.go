// Package stats holds the streaming sketches and statistical primitives the
// analyzer is built on. Everything here is dependency-free and deterministic.
package stats

import (
	"math"
	"sort"
)

// Centroid is a weighted point in a t-digest.
type Centroid struct {
	Mean   float64 `json:"m"`
	Weight float64 `json:"w"`
}

// TDigest is a merging t-digest (Dunning & Ertl). Memory is bounded by the
// compression parameter, independent of how many values are added.
type TDigest struct {
	compression float64
	centroids   []Centroid // sorted by mean, fully merged
	buf         []Centroid // unmerged additions
	weight      float64    // total weight of centroids (excludes buf)
	min, max    float64
}

// DefaultCompression keeps roughly 100-200 centroids, enough for quantile
// error well under 1% at the tails.
const DefaultCompression = 100

// NewTDigest returns an empty digest. A compression <= 0 uses the default.
func NewTDigest(compression float64) *TDigest {
	if compression <= 0 {
		compression = DefaultCompression
	}
	return &TDigest{compression: compression, min: math.Inf(1), max: math.Inf(-1)}
}

// Add records one value. NaN and infinities are ignored; callers count them
// separately as invalid outputs.
func (t *TDigest) Add(x float64) { t.AddWeighted(x, 1) }

// AddWeighted records a value with the given weight.
func (t *TDigest) AddWeighted(x, w float64) {
	if math.IsNaN(x) || math.IsInf(x, 0) || w <= 0 {
		return
	}
	t.buf = append(t.buf, Centroid{x, w})
	if x < t.min {
		t.min = x
	}
	if x > t.max {
		t.max = x
	}
	if len(t.buf) >= t.bufferCap() {
		t.compress()
	}
}

// Merge folds another digest into this one.
func (t *TDigest) Merge(o *TDigest) {
	if o == nil {
		return
	}
	o.compress()
	if o.weight == 0 {
		return
	}
	t.buf = append(t.buf, o.centroids...)
	t.min = math.Min(t.min, o.min)
	t.max = math.Max(t.max, o.max)
	if len(t.buf) >= t.bufferCap() {
		t.compress()
	}
}

// Count is the total weight added.
func (t *TDigest) Count() float64 {
	t.compress()
	return t.weight
}

// Min and Max are exact.
func (t *TDigest) Min() float64 { return t.min }
func (t *TDigest) Max() float64 { return t.max }

// Centroids returns a copy of the merged centroids.
func (t *TDigest) Centroids() []Centroid {
	t.compress()
	return append([]Centroid(nil), t.centroids...)
}

func (t *TDigest) bufferCap() int { return int(t.compression) * 5 }

// k1 scale function and its inverse: centroids get small near the tails.
func (t *TDigest) k(q float64) float64 {
	return t.compression / (2 * math.Pi) * math.Asin(2*q-1)
}

func (t *TDigest) kInv(k float64) float64 {
	if k >= t.compression/4 {
		return 1
	}
	return (math.Sin(2*math.Pi*k/t.compression) + 1) / 2
}

func (t *TDigest) compress() {
	if len(t.buf) == 0 {
		return
	}
	all := append(t.centroids, t.buf...)
	t.buf = t.buf[:0]
	sort.Slice(all, func(i, j int) bool { return all[i].Mean < all[j].Mean })
	total := 0.0
	for _, c := range all {
		total += c.Weight
	}
	merged := make([]Centroid, 0, int(t.compression))
	cur := all[0]
	soFar := 0.0
	qLimit := t.kInv(t.k(0) + 1)
	for _, c := range all[1:] {
		if (soFar+cur.Weight+c.Weight)/total <= qLimit {
			w := cur.Weight + c.Weight
			cur.Mean += (c.Mean - cur.Mean) * c.Weight / w
			cur.Weight = w
			continue
		}
		merged = append(merged, cur)
		soFar += cur.Weight
		qLimit = t.kInv(t.k(soFar/total) + 1)
		cur = c
	}
	merged = append(merged, cur)
	t.centroids = merged
	t.weight = total
}

// Quantile returns the estimated value at quantile q in [0, 1].
func (t *TDigest) Quantile(q float64) float64 {
	t.compress()
	n := len(t.centroids)
	if n == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return t.min
	}
	if q >= 1 {
		return t.max
	}
	if n == 1 {
		return t.centroids[0].Mean
	}
	target := q * t.weight
	// Interpolate between centroid centers, anchored at min and max.
	prevPos, prevVal := 0.0, t.min
	cum := 0.0
	for _, c := range t.centroids {
		center := cum + c.Weight/2
		if target < center {
			return lerp(prevPos, prevVal, center, c.Mean, target)
		}
		prevPos, prevVal = center, c.Mean
		cum += c.Weight
	}
	return lerp(prevPos, prevVal, t.weight, t.max, target)
}

// CDF returns the estimated fraction of values <= x.
func (t *TDigest) CDF(x float64) float64 {
	t.compress()
	n := len(t.centroids)
	if n == 0 {
		return math.NaN()
	}
	if x < t.min {
		return 0
	}
	if x >= t.max {
		return 1
	}
	prevPos, prevVal := 0.0, t.min
	cum := 0.0
	for _, c := range t.centroids {
		center := cum + c.Weight/2
		if x < c.Mean {
			return lerpInv(prevVal, prevPos, c.Mean, center, x) / t.weight
		}
		prevPos, prevVal = center, c.Mean
		cum += c.Weight
	}
	return lerpInv(prevVal, prevPos, t.max, t.weight, x) / t.weight
}

// lerp maps pos in [x0,x1] onto [y0,y1].
func lerp(x0, y0, x1, y1, pos float64) float64 {
	if x1 == x0 {
		return y1
	}
	return y0 + (y1-y0)*(pos-x0)/(x1-x0)
}

// lerpInv maps value v in [v0,v1] onto positions [p0,p1].
func lerpInv(v0, p0, v1, p1, v float64) float64 {
	if v1 == v0 {
		return p1
	}
	return p0 + (p1-p0)*(v-v0)/(v1-v0)
}

// KS returns the two-sample Kolmogorov-Smirnov statistic between digests.
func KS(a, b *TDigest) float64 {
	if a.Count() == 0 || b.Count() == 0 {
		return math.NaN()
	}
	points := []float64{a.Min(), a.Max(), b.Min(), b.Max()}
	for _, c := range a.Centroids() {
		points = append(points, c.Mean)
	}
	for _, c := range b.Centroids() {
		points = append(points, c.Mean)
	}
	d := 0.0
	for _, x := range points {
		d = math.Max(d, math.Abs(a.CDF(x)-b.CDF(x)))
	}
	return d
}

// Wasserstein returns the 1-Wasserstein (earth mover's) distance, computed as
// the integral of the absolute difference between quantile functions.
func Wasserstein(a, b *TDigest) float64 {
	if a.Count() == 0 || b.Count() == 0 {
		return math.NaN()
	}
	const steps = 400
	sum := 0.0
	for i := 0; i < steps; i++ {
		q := (float64(i) + 0.5) / steps
		sum += math.Abs(a.Quantile(q) - b.Quantile(q))
	}
	return sum / steps
}
