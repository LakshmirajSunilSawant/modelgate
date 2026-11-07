// Package audit records rollout decisions in a durable log: every verdict,
// step change, promotion, rollback, and manual action, with who made it.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Actions recorded in the log.
const (
	ActionDeployed        = "Deployed"
	ActionRolloutStarted  = "RolloutStarted"
	ActionCanaryReplaced  = "CanaryReplaced"
	ActionCanaryCancelled = "CanaryCancelled"
	ActionVerdict         = "Verdict"
	ActionStepAdvanced    = "StepAdvanced"
	ActionPromoted        = "Promoted"
	ActionForcePromoted   = "ForcePromoted"
	ActionRolledBack      = "RolledBack"
	ActionHeld            = "Held"
	ActionResumed         = "Resumed"
)

// ControllerActor is the actor for decisions the controller makes on its own.
const ControllerActor = "system:modelgate-controller"

// Entry is one audit record.
type Entry struct {
	// Key makes writes idempotent: an entry that is written again after a
	// retried reconcile is stored once. Empty means the time is used instead.
	Key            string
	Time           time.Time
	Namespace      string
	Rollout        string
	RolloutUID     string
	Action         string
	Actor          string
	StableRevision string
	CanaryRevision string
	Step           int32
	CanaryWeight   int32
	Reason         string
	// Verdict is the analyzer verdict as stored on the rollout status,
	// including up to five disagreement samples. Nil for non-verdict entries.
	Verdict json.RawMessage
	// Blocking entries must be stored before the decision takes effect.
	// Promotions and step advances are blocking; rollbacks never are, so an
	// audit outage can never keep a bad canary in place.
	Blocking bool
}

// ID is the primary key the entry is stored under.
func (e Entry) ID() string {
	key := e.Key
	if key == "" {
		key = e.Time.UTC().Format(time.RFC3339Nano)
	}
	h := sha256.Sum256([]byte(strings.Join([]string{
		e.RolloutUID, e.Namespace, e.Rollout, e.Action, e.StableRevision, e.CanaryRevision, key,
	}, "\x00")))
	return hex.EncodeToString(h[:16])
}

// Log stores entries.
type Log interface {
	// Write stores entries atomically. Entries already stored are skipped.
	Write(ctx context.Context, entries []Entry) error
	// Ping reports whether the log can accept writes right now.
	Ping(ctx context.Context) error
}

// Noop discards entries. It is used when no audit database is configured;
// Kubernetes events still record every decision.
type Noop struct{}

func (Noop) Write(context.Context, []Entry) error { return nil }
func (Noop) Ping(context.Context) error           { return nil }

// Memory keeps entries in memory, for tests.
type Memory struct {
	Entries []Entry
	Err     error
	seen    map[string]bool
}

func (m *Memory) Write(_ context.Context, entries []Entry) error {
	if m.Err != nil {
		return m.Err
	}
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	for _, e := range entries {
		if id := e.ID(); !m.seen[id] {
			m.seen[id] = true
			m.Entries = append(m.Entries, e)
		}
	}
	return nil
}

func (m *Memory) Ping(context.Context) error { return m.Err }

// Actions lists the recorded actions in order, for assertions.
func (m *Memory) Actions() []string {
	out := make([]string, len(m.Entries))
	for i, e := range m.Entries {
		out[i] = e.Action
	}
	return out
}

// ErrUnavailable wraps write and ping failures.
type ErrUnavailable struct{ Err error }

func (e ErrUnavailable) Error() string { return fmt.Sprintf("audit log unavailable: %v", e.Err) }
func (e ErrUnavailable) Unwrap() error { return e.Err }
