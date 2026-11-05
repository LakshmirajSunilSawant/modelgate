package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/modelgate/modelgate/internal/analysis"
)

// Analyzer returns a verdict for a canary. The controller treats any error
// as "no evidence" and holds the rollout where it is.
type Analyzer interface {
	Verdict(ctx context.Context, req analysis.Request) (analysis.Verdict, error)
}

// HTTPAnalyzer calls the analyzer service's /v1/verdict endpoint.
type HTTPAnalyzer struct {
	BaseURL string
	Client  *http.Client
}

func (a *HTTPAnalyzer) Verdict(ctx context.Context, req analysis.Request) (analysis.Verdict, error) {
	var v analysis.Verdict
	body, err := json.Marshal(req)
	if err != nil {
		return v, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.BaseURL+"/v1/verdict", bytes.NewReader(body))
	if err != nil {
		return v, err
	}
	hreq.Header.Set("Content-Type", "application/json")
	c := a.Client
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(hreq)
	if err != nil {
		return v, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return v, fmt.Errorf("analyzer returned %s: %s", resp.Status, bytes.TrimSpace(msg))
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return v, fmt.Errorf("decoding verdict: %w", err)
	}
	if v.Decision != analysis.Pass && v.Decision != analysis.Fail && v.Decision != analysis.Inconclusive {
		return v, fmt.Errorf("analyzer returned unknown decision %q", v.Decision)
	}
	return v, nil
}
