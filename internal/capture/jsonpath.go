// Package capture implements the sidecar reverse proxy that records
// predictions from model responses without changing the model server.
package capture

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Path is a compiled subset of JSONPath: $.a.b[0].c and $['a'].
// This covers the response shapes of common HTTP model servers
// (KServe v1/v2, BentoML, FastAPI) without a general JSONPath engine.
type Path struct {
	raw   string
	steps []step
}

type step struct {
	key   string
	index int
	isIdx bool
}

// ParsePath compiles an expression. An empty expression yields a nil path.
func ParsePath(expr string) (*Path, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}
	if !strings.HasPrefix(expr, "$") {
		return nil, fmt.Errorf("jsonpath %q: must start with $", expr)
	}
	p := &Path{raw: expr}
	rest := expr[1:]
	for len(rest) > 0 {
		switch rest[0] {
		case '.':
			rest = rest[1:]
			end := strings.IndexAny(rest, ".[")
			if end < 0 {
				end = len(rest)
			}
			if end == 0 {
				return nil, fmt.Errorf("jsonpath %q: empty key", expr)
			}
			p.steps = append(p.steps, step{key: rest[:end]})
			rest = rest[end:]
		case '[':
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				return nil, fmt.Errorf("jsonpath %q: unclosed [", expr)
			}
			inner := rest[1:end]
			rest = rest[end+1:]
			if len(inner) >= 2 && (inner[0] == '\'' || inner[0] == '"') && inner[len(inner)-1] == inner[0] {
				p.steps = append(p.steps, step{key: inner[1 : len(inner)-1]})
				continue
			}
			i, err := strconv.Atoi(inner)
			if err != nil || i < 0 {
				return nil, fmt.Errorf("jsonpath %q: bad index [%s]", expr, inner)
			}
			p.steps = append(p.steps, step{index: i, isIdx: true})
		default:
			return nil, fmt.Errorf("jsonpath %q: unexpected %q", expr, rest[0])
		}
	}
	return p, nil
}

func (p *Path) String() string { return p.raw }

// Get resolves the path against a decoded JSON document.
func (p *Path) Get(doc any) (any, bool) {
	cur := doc
	for _, s := range p.steps {
		if s.isIdx {
			arr, ok := cur.([]any)
			if !ok || s.index >= len(arr) {
				return nil, false
			}
			cur = arr[s.index]
			continue
		}
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = obj[s.key]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// labelString renders a JSON value as a class label.
func labelString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case json.Number:
		return t.String(), true
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), true
	}
	return "", false
}

// scoreFloat reads a JSON value as a numeric score.
func scoreFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case float64:
		return t, true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	}
	return 0, false
}
