package capture

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Redacted replaces every value a redact path matches.
const Redacted = "[REDACTED]"

// maxPayloadsPerSecond caps payload capture per sidecar, so a high sample
// rate cannot flood the event pipeline with bodies.
const maxPayloadsPerSecond = 20

// PayloadConfig turns on raw payload capture until a deadline.
type PayloadConfig struct {
	Until      time.Time // capture stops at this time; zero means off
	SampleRate float64   // share of captured requests whose bodies are kept
	MaxBytes   int       // largest body kept
	Redact     []*Path
}

// ParseRedactPaths compiles redact paths. Each must name a field: "$" alone
// would redact the whole body, which is better done by not capturing it.
func ParseRedactPaths(exprs []string) ([]*Path, error) {
	var out []*Path
	for _, e := range exprs {
		p, err := ParsePath(e)
		if err != nil {
			return nil, err
		}
		if p == nil || len(p.steps) == 0 {
			return nil, fmt.Errorf("redact path %q must name a field", e)
		}
		out = append(out, p)
	}
	return out, nil
}

// Redact replaces every value the path matches with Redacted, in place,
// and returns how many values it replaced.
func (p *Path) Redact(doc any) int {
	if len(p.steps) == 0 {
		return 0
	}
	return redact(doc, p.steps)
}

func redact(cur any, steps []step) int {
	s, last := steps[0], len(steps) == 1
	visit := func(v any, set func()) int {
		if last {
			set()
			return 1
		}
		return redact(v, steps[1:])
	}
	n := 0
	switch c := cur.(type) {
	case map[string]any:
		if s.wild {
			for k, v := range c {
				n += visit(v, func() { c[k] = Redacted })
			}
		} else if v, ok := c[s.key]; ok && !s.isIdx {
			n += visit(v, func() { c[s.key] = Redacted })
		}
	case []any:
		if s.wild {
			for i, v := range c {
				n += visit(v, func() { c[i] = Redacted })
			}
		} else if s.isIdx && s.index < len(c) {
			n += visit(c[s.index], func() { c[s.index] = Redacted })
		}
	}
	return n
}

// redactBody parses a JSON body, applies the redact paths and re-encodes
// it. A body that cannot be parsed cannot be redacted, so it is left out and
// the second result says why. Nothing is returned for an empty body.
func redactBody(body []byte, overflow bool, cfg *PayloadConfig) (json.RawMessage, string) {
	if overflow {
		return nil, fmt.Sprintf("larger than %d bytes", cfg.MaxBytes)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, ""
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return nil, "not JSON, so it cannot be redacted"
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, "not JSON, so it cannot be redacted"
	}
	for _, p := range cfg.Redact {
		p.Redact(doc)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, "could not be re-encoded after redaction"
	}
	if len(out) > cfg.MaxBytes {
		return nil, fmt.Sprintf("larger than %d bytes", cfg.MaxBytes)
	}
	return out, ""
}

// payloadLimiter allows at most a fixed number of payloads per second.
type payloadLimiter struct {
	mu     sync.Mutex
	second int64
	used   int
	denied atomic.Int64
}

func (l *payloadLimiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if s := now.Unix(); s != l.second {
		l.second, l.used = s, 0
	}
	if l.used >= maxPayloadsPerSecond {
		l.denied.Add(1)
		return false
	}
	l.used++
	return true
}

// boundedBody passes a request body through to the model server while
// keeping the first limit bytes. The transport may still be writing the
// body when the response arrives, so access is locked.
type boundedBody struct {
	src      io.ReadCloser
	limit    int
	mu       sync.Mutex
	buf      bytes.Buffer
	overflow bool
}

func (b *boundedBody) Read(p []byte) (int, error) {
	n, err := b.src.Read(p)
	if n > 0 {
		b.mu.Lock()
		if !b.overflow {
			if b.buf.Len()+n > b.limit {
				b.overflow = true
				b.buf = bytes.Buffer{}
			} else {
				b.buf.Write(p[:n])
			}
		}
		b.mu.Unlock()
	}
	return n, err
}

func (b *boundedBody) Close() error { return b.src.Close() }

// snapshot returns a copy of what has been read so far.
func (b *boundedBody) snapshot() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes()), b.overflow
}
