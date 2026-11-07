package controller

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/audit"
)

func withAudit(e *env) *audit.Memory {
	m := &audit.Memory{}
	e.r.Audit = m
	return m
}

func TestAuditRecordsHappyPath(t *testing.T) {
	e := newEnv(t)
	log := withAudit(e)
	_, newRev := e.startRollout()
	e.reconcile() // shadow verdict
	for i := 0; i < 3; i++ {
		e.clock = e.clock.Add(11 * time.Minute)
		e.reconcile()
	}
	if st := e.get().Status; st.StableRevision != newRev {
		t.Fatalf("not promoted: %+v", st)
	}
	want := []string{
		audit.ActionDeployed, audit.ActionRolloutStarted,
		audit.ActionVerdict, // shadow, time remaining
		audit.ActionVerdict, audit.ActionStepAdvanced,
		audit.ActionVerdict, audit.ActionStepAdvanced,
		audit.ActionVerdict, audit.ActionPromoted,
	}
	if got := log.Actions(); !slices.Equal(got, want) {
		t.Fatalf("actions\n got %v\nwant %v", got, want)
	}
	last := log.Entries[len(log.Entries)-1]
	if last.Actor != audit.ControllerActor || last.CanaryRevision != newRev || last.Namespace != "ml" || last.RolloutUID != "uid-1" {
		t.Errorf("promote entry %+v", last)
	}
	if v := log.Entries[2]; len(v.Verdict) == 0 {
		t.Error("verdict entry has no verdict body")
	}
}

func TestAuditRecordsManualActorsAndPause(t *testing.T) {
	e := newEnv(t)
	log := withAudit(e)
	e.startRollout()
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.Paused = true
		mr.Annotations = map[string]string{mgv1.ActionByAnnotation: "bob@example.com"}
	})
	e.reconcile()
	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Paused = false })
	e.reconcile()
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Annotations = map[string]string{mgv1.ActionAnnotation: "abort", mgv1.ActionByAnnotation: "alice@example.com"}
	})
	e.reconcile()

	byAction := map[string]audit.Entry{}
	for _, en := range log.Entries {
		byAction[en.Action] = en
	}
	if en := byAction[audit.ActionHeld]; en.Actor != "bob@example.com" {
		t.Errorf("pause actor %q", en.Actor)
	}
	if en := byAction[audit.ActionResumed]; en.Actor != "bob@example.com" {
		t.Errorf("resume entry %+v", en)
	}
	if en := byAction[audit.ActionRolledBack]; en.Actor != "alice@example.com" {
		t.Errorf("abort actor %q", en.Actor)
	}
}

// With the audit log down, a rollout must not advance or promote, but a
// failing canary must still be rolled back.
func TestAuditDownHoldsButStillRollsBack(t *testing.T) {
	e := newEnv(t)
	log := withAudit(e)
	e.startRollout()
	log.Err = errors.New("connection refused")
	for i := 0; i < 3; i++ {
		e.clock = e.clock.Add(time.Hour)
		e.reconcile()
	}
	st := e.get().Status
	if st.Phase != mgv1.PhasePaused || st.CurrentStep != mgv1.ShadowStepIndex {
		t.Fatalf("advanced without audit: %+v", st)
	}

	e.an.verdict = analysis.Verdict{Decision: analysis.Fail, HardFailure: true, Reason: "valid: 40 of 2000 outputs invalid"}
	e.clock = e.clock.Add(time.Hour)
	e.reconcile()
	if st := e.get().Status; st.Phase != mgv1.PhaseDegraded || st.CanaryRevision != "" {
		t.Fatalf("rollback blocked by audit outage: %+v", st)
	}
}

func TestAuditDownBlocksForcePromote(t *testing.T) {
	e := newEnv(t)
	log := withAudit(e)
	_, newRev := e.startRollout()
	log.Err = errors.New("connection refused")
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Annotations = map[string]string{mgv1.ActionAnnotation: "promote", mgv1.ActionByAnnotation: "alice@example.com"}
	})
	if _, err := e.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: e.key}); err == nil {
		t.Fatal("force promote succeeded without an audit record")
	}
	mr := e.get()
	if mr.Status.StableRevision == newRev || mr.Annotations[mgv1.ActionAnnotation] != "promote" {
		t.Fatalf("promote applied or annotation dropped: %+v", mr.Status)
	}

	log.Err = nil // database back: the pending action now goes through, once
	e.reconcile()
	if st := e.get().Status; st.StableRevision != newRev {
		t.Fatalf("not promoted after recovery: %+v", st)
	}
	n := 0
	for _, en := range log.Entries {
		if en.Action == audit.ActionForcePromoted && en.Actor == "alice@example.com" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("force promote recorded %d times", n)
	}
}
