package stats

import "math"

// The analyzer looks at the data repeatedly while a rollout step is running.
// A fixed-sample test applied at every look would inflate false rollbacks, so
// every bound here is either anytime-valid (a confidence sequence) or uses an
// explicit alpha-spending schedule across looks.

// LookAlpha spends a total false-alarm budget alpha across an unbounded number
// of looks: look k (1-based) gets alpha/(k(k+1)). The sum over all k is alpha,
// so by the union bound the chance of any false alarm during a stage is at
// most alpha, no matter how often the controller polls.
func LookAlpha(alpha float64, look int) float64 {
	if look < 1 {
		look = 1
	}
	k := float64(look)
	return alpha / (k * (k + 1))
}

// BernoulliBounds returns a two-sided confidence interval for a proportion
// from k successes in n trials using the Chernoff (binomial KL) inequality:
// P(p_hat deviates to x) <= exp(-n KL(x || p)). Unlike Hoeffding, the width
// adapts to the variance, so rates near 0 or 1 (error rates, agreement rates)
// get tight bounds. Combine with LookAlpha when checking repeatedly.
func BernoulliBounds(k, n int64, alpha float64) (lo, hi float64) {
	if n <= 0 {
		return 0, 1
	}
	ph := float64(k) / float64(n)
	limit := math.Log(2/alpha) / float64(n)
	kl := func(p float64) float64 { return bernoulliKL(ph, p) }
	lo, hi = 0, 1
	if ph > 0 && kl(0) > limit {
		lo = bisect(0, ph, func(p float64) bool { return kl(p) > limit })
	}
	if ph < 1 && kl(1) > limit {
		hi = bisect(1, ph, func(p float64) bool { return kl(p) > limit })
	}
	return lo, hi
}

// bisect narrows from the excluded endpoint `out` towards `in` and returns
// the boundary where excluded(p) stops holding.
func bisect(out, in float64, excluded func(float64) bool) float64 {
	for i := 0; i < 60; i++ {
		mid := (out + in) / 2
		if excluded(mid) {
			out = mid
		} else {
			in = mid
		}
	}
	return in
}

// bernoulliKL is KL(Bern(a) || Bern(b)).
func bernoulliKL(a, b float64) float64 {
	const eps = 1e-15
	b = math.Min(math.Max(b, eps), 1-eps)
	v := 0.0
	if a > 0 {
		v += a * math.Log(a/b)
	}
	if a < 1 {
		v += (1 - a) * math.Log((1-a)/(1-b))
	}
	return v
}

// DKW returns the Dvoretzky-Kiefer-Wolfowitz radius: with probability at least
// 1-alpha the empirical CDF of n samples is within eps of the true CDF
// everywhere.
func DKW(n float64, alpha float64) float64 {
	if n <= 0 {
		return math.Inf(1)
	}
	return math.Sqrt(math.Log(2/alpha) / (2 * n))
}

// NormalQuantile returns the inverse of the standard normal CDF (Acklam's
// rational approximation, relative error < 1.2e-9).
func NormalQuantile(p float64) float64 {
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	a := [...]float64{-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00}
	b := [...]float64{-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01}
	c := [...]float64{-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00}
	d := [...]float64{7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00}
	const lo, hi = 0.02425, 1 - 0.02425
	switch {
	case p < lo:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	case p > hi:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	default:
		q := p - 0.5
		r := q * q
		return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r + a[5]) * q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r + 1)
	}
}

// QuantileBand returns the range of quantile levels [lo, hi] that contains
// the true q-quantile's rank with probability 1-alpha, using the normal
// approximation to the binomial distribution of order statistics.
func QuantileBand(q, n, alpha float64) (lo, hi float64) {
	if n <= 0 {
		return 0, 1
	}
	z := NormalQuantile(1 - alpha/2)
	h := z * math.Sqrt(q*(1-q)/n)
	return math.Max(0, q-h), math.Min(1, q+h)
}

// ChiSquareSF is the survival function P(X > x) of a chi-square distribution
// with k degrees of freedom.
func ChiSquareSF(x, k float64) float64 {
	if x <= 0 {
		return 1
	}
	return gammaQ(k/2, x/2)
}

// gammaQ is the regularized upper incomplete gamma function Q(a, x).
func gammaQ(a, x float64) float64 {
	if x < a+1 {
		return 1 - gammaPSeries(a, x)
	}
	return gammaQContinuedFraction(a, x)
}

func gammaPSeries(a, x float64) float64 {
	lg, _ := math.Lgamma(a)
	sum := 1 / a
	term := sum
	for n := 1; n < 1000; n++ {
		term *= x / (a + float64(n))
		sum += term
		if math.Abs(term) < math.Abs(sum)*1e-15 {
			break
		}
	}
	return sum * math.Exp(-x+a*math.Log(x)-lg)
}

func gammaQContinuedFraction(a, x float64) float64 {
	const tiny = 1e-300
	lg, _ := math.Lgamma(a)
	b := x + 1 - a
	c := 1 / tiny
	d := 1 / b
	h := d
	for i := 1; i < 1000; i++ {
		an := -float64(i) * (float64(i) - a)
		b += 2
		d = an*d + b
		if math.Abs(d) < tiny {
			d = tiny
		}
		c = b + an/c
		if math.Abs(c) < tiny {
			c = tiny
		}
		d = 1 / d
		del := d * c
		h *= del
		if math.Abs(del-1) < 1e-15 {
			break
		}
	}
	return math.Exp(-x+a*math.Log(x)-lg) * h
}

// ContingencyChiSquare runs a chi-square test of homogeneity on a 2 x k table
// of counts and returns the statistic, degrees of freedom, p-value, and
// Cramér's V effect size. Categories empty in both rows are dropped.
func ContingencyChiSquare(a, b []float64) (stat float64, df int, p float64, cramersV float64) {
	var na, nb float64
	for i := range a {
		na += a[i]
		nb += b[i]
	}
	n := na + nb
	if na == 0 || nb == 0 {
		return 0, 0, 1, 0
	}
	cats := 0
	for i := range a {
		col := a[i] + b[i]
		if col == 0 {
			continue
		}
		cats++
		ea := na * col / n
		eb := nb * col / n
		stat += (a[i]-ea)*(a[i]-ea)/ea + (b[i]-eb)*(b[i]-eb)/eb
	}
	df = cats - 1
	if df < 1 {
		return 0, 0, 1, 0
	}
	return stat, df, ChiSquareSF(stat, float64(df)), math.Sqrt(stat / n)
}

// PSI is the population stability index between two count vectors, with a
// small floor on proportions so empty bins do not produce infinities.
func PSI(expected, actual []float64) float64 {
	var ne, na float64
	for i := range expected {
		ne += expected[i]
		na += actual[i]
	}
	if ne == 0 || na == 0 {
		return math.NaN()
	}
	const floor = 1e-4
	psi := 0.0
	for i := range expected {
		e := math.Max(expected[i]/ne, floor)
		a := math.Max(actual[i]/na, floor)
		psi += (a - e) * math.Log(a/e)
	}
	return psi
}
