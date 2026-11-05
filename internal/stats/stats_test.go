package stats

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestTDigestQuantilesNormal(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	td := NewTDigest(0)
	vals := make([]float64, 100000)
	for i := range vals {
		vals[i] = r.NormFloat64()
		td.Add(vals[i])
	}
	sort.Float64s(vals)
	// Tolerance widens at the extreme tail, where the sample quantile itself is noisy.
	tol := map[float64]float64{0.01: 0.03, 0.1: 0.03, 0.5: 0.03, 0.9: 0.03, 0.99: 0.03, 0.999: 0.06}
	for q, eps := range tol {
		want := vals[int(q*float64(len(vals)))]
		got := td.Quantile(q)
		if math.Abs(got-want) > eps {
			t.Errorf("q=%v: got %.4f want %.4f", q, got, want)
		}
	}
	if n := len(td.Centroids()); n > 300 {
		t.Errorf("digest not bounded: %d centroids", n)
	}
	if c := td.CDF(0); math.Abs(c-0.5) > 0.01 {
		t.Errorf("CDF(0)=%v", c)
	}
}

func TestTDigestMerge(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	a, b, all := NewTDigest(0), NewTDigest(0), NewTDigest(0)
	for i := 0; i < 20000; i++ {
		x := r.ExpFloat64()
		all.Add(x)
		if i%2 == 0 {
			a.Add(x)
		} else {
			b.Add(x)
		}
	}
	m := NewTDigest(0)
	m.Merge(a)
	m.Merge(b)
	if m.Count() != 20000 {
		t.Fatalf("count %v", m.Count())
	}
	for _, q := range []float64{0.5, 0.9, 0.99} {
		if d := math.Abs(m.Quantile(q) - all.Quantile(q)); d > 0.05 {
			t.Errorf("q=%v merged off by %v", q, d)
		}
	}
}

func TestKSAndWasserstein(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	a, b, c := NewTDigest(0), NewTDigest(0), NewTDigest(0)
	for i := 0; i < 20000; i++ {
		a.Add(r.NormFloat64())
		b.Add(r.NormFloat64())
		c.Add(r.NormFloat64() + 1)
	}
	if d := KS(a, b); d > 0.03 {
		t.Errorf("same-dist KS too large: %v", d)
	}
	// KS between N(0,1) and N(1,1) is 2*Phi(0.5)-1 ≈ 0.383.
	if d := KS(a, c); math.Abs(d-0.383) > 0.03 {
		t.Errorf("shifted KS = %v", d)
	}
	if w := Wasserstein(a, c); math.Abs(w-1) > 0.05 {
		t.Errorf("shifted W = %v", w)
	}
}

func TestBernoulliBoundsWithLookSpending(t *testing.T) {
	// Check every 100 observations with alpha spent across looks; the share of
	// runs where the true rate ever falls outside the interval must stay
	// below alpha.
	r := rand.New(rand.NewSource(4))
	const alpha, runs, n = 0.05, 400, 5000
	for _, p := range []float64{0.01, 0.5, 0.97} {
		miss := 0
		for run := 0; run < runs; run++ {
			var k int64
			look := 0
			for i := int64(1); i <= n; i++ {
				if r.Float64() < p {
					k++
				}
				if i%100 != 0 {
					continue
				}
				look++
				lo, hi := BernoulliBounds(k, i, LookAlpha(alpha, look))
				if p < lo || p > hi {
					miss++
					break
				}
			}
		}
		if rate := float64(miss) / runs; rate > alpha {
			t.Errorf("p=%v: miss rate %.3f exceeds alpha %.3f", p, rate, alpha)
		}
	}
	lo, hi := BernoulliBounds(97, 100, 0.05)
	if !(lo < 0.97 && hi > 0.97 && hi <= 1 && lo > 0.85) {
		t.Errorf("bounds for 97/100: [%v, %v]", lo, hi)
	}
}

func TestLookAlphaSums(t *testing.T) {
	sum := 0.0
	for k := 1; k <= 100000; k++ {
		sum += LookAlpha(0.01, k)
	}
	if sum > 0.01 {
		t.Errorf("spent %v > budget", sum)
	}
}

func TestNormalQuantile(t *testing.T) {
	cases := map[float64]float64{0.5: 0, 0.975: 1.959964, 0.995: 2.575829, 0.01: -2.326348}
	for p, want := range cases {
		if got := NormalQuantile(p); math.Abs(got-want) > 1e-5 {
			t.Errorf("NormalQuantile(%v)=%v want %v", p, got, want)
		}
	}
}

func TestChiSquareSF(t *testing.T) {
	// Reference values from scipy.stats.chi2.sf.
	cases := []struct{ x, k, want float64 }{
		{3.841459, 1, 0.05},
		{11.0705, 5, 0.05},
		{1, 2, 0.606531},
		{50, 10, 2.6690e-07},
	}
	for _, c := range cases {
		got := ChiSquareSF(c.x, c.k)
		if math.Abs(got-c.want) > 1e-4*math.Max(1, c.want*1e3) && math.Abs(got-c.want)/c.want > 1e-3 {
			t.Errorf("ChiSquareSF(%v,%v)=%v want %v", c.x, c.k, got, c.want)
		}
	}
}

func TestPSIAndChiSquare(t *testing.T) {
	same := PSI([]float64{900, 100}, []float64{905, 95})
	if same > 0.01 {
		t.Errorf("PSI on near-identical = %v", same)
	}
	shift := PSI([]float64{900, 100}, []float64{700, 300}) // 3x more fraud flags
	if shift < 0.1 {
		t.Errorf("PSI on 3x shift = %v", shift)
	}
	_, df, p, v := ContingencyChiSquare([]float64{970, 30}, []float64{910, 90})
	if df != 1 || p > 1e-6 || v < 0.1 {
		t.Errorf("chi2 df=%d p=%v v=%v", df, p, v)
	}
}
