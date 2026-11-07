package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

// sidecarEnv returns the capture container's env for one revision.
func (e *env) sidecarEnv(rev string) map[string]string {
	e.t.Helper()
	var d appsv1.Deployment
	if err := e.c.Get(context.Background(), types.NamespacedName{Namespace: "ml", Name: "fraud-" + rev}, &d); err != nil {
		e.t.Fatal(err)
	}
	out := map[string]string{}
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == sidecarName {
			for _, v := range c.Env {
				out[v.Name] = v.Value
			}
		}
	}
	return out
}

func (e *env) payloadCondition() *metav1.Condition {
	return findCondition(e.get(), mgv1.ConditionPayloadCapture)
}

func TestPayloadCaptureReachesBothSidecarsAndEnds(t *testing.T) {
	e := newEnv(t)
	oldRev, newRev := e.startRollout()
	until := metav1.NewTime(e.clock.Add(2 * time.Hour))
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.Capture.Payloads = &mgv1.PayloadCaptureSpec{Until: until, SampleRate: 0.1, Redact: []string{"$.ssn"}}
	})
	e.reconcile()
	if c := e.payloadCondition(); c == nil || c.Reason != payloadActive {
		t.Fatalf("condition %+v", c)
	}
	for _, rev := range []string{oldRev, newRev} {
		env := e.sidecarEnv(rev)
		if env["MODELGATE_PAYLOAD_UNTIL"] != "2026-11-01T14:00:00Z" || env["MODELGATE_PAYLOAD_REDACT"] != `["$.ssn"]` ||
			env["MODELGATE_PAYLOAD_SAMPLE_RATE"] != "0.1" {
			t.Errorf("revision %s sidecar env %v", rev, env)
		}
	}
	if res := e.reconcile(); res.RequeueAfter <= 0 || res.RequeueAfter > 2*time.Hour+time.Second {
		t.Errorf("requeue %v, want at most the time until capture ends", res.RequeueAfter)
	}

	// At the deadline the sidecar stops by itself. The controller keeps its
	// settings, so expiry restarts no pods.
	before := e.sidecarEnv(newRev)
	e.clock = until.Add(time.Minute)
	e.reconcile()
	if c := e.payloadCondition(); c == nil || c.Reason != payloadExpired || c.Status != metav1.ConditionFalse {
		t.Errorf("after the deadline: %+v", c)
	}
	if after := e.sidecarEnv(newRev); after["MODELGATE_PAYLOAD_UNTIL"] != before["MODELGATE_PAYLOAD_UNTIL"] {
		t.Error("sidecar settings changed at expiry")
	}

	e.update(func(mr *mgv1.ModelRollout) { mr.Spec.Capture.Payloads = nil })
	e.reconcile()
	if c := e.payloadCondition(); c != nil {
		t.Errorf("condition left after removing payloads: %+v", c)
	}
	if _, ok := e.sidecarEnv(newRev)["MODELGATE_PAYLOAD_UNTIL"]; ok {
		t.Error("sidecar still configured for payload capture")
	}
}

func TestPayloadCaptureRefusesLongOrPastWindows(t *testing.T) {
	for name, tc := range map[string]struct {
		until  time.Duration
		redact []string
		reason string
	}{
		"too far ahead": {until: 25 * time.Hour, reason: payloadTooLong},
		"in the past":   {until: -time.Hour, reason: payloadPast},
		"bad path":      {until: time.Hour, redact: []string{"ssn"}, reason: payloadBadConfig},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			_, newRev := e.startRollout()
			e.update(func(mr *mgv1.ModelRollout) {
				mr.Spec.Capture.Payloads = &mgv1.PayloadCaptureSpec{Until: metav1.NewTime(e.clock.Add(tc.until)), Redact: tc.redact}
			})
			e.reconcile()
			if c := e.payloadCondition(); c == nil || c.Reason != tc.reason || c.Status != metav1.ConditionFalse {
				t.Errorf("condition %+v, want %s", c, tc.reason)
			}
			if _, ok := e.sidecarEnv(newRev)["MODELGATE_PAYLOAD_UNTIL"]; ok {
				t.Error("sidecar configured for payload capture")
			}
		})
	}
}

func TestPolicyCanDisablePayloadCapture(t *testing.T) {
	e := newEnv(t)
	_, newRev := e.startRollout()
	pol := &mgv1.ModelGatePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ml", Name: mgv1.PolicyName},
		Spec:       mgv1.ModelGatePolicySpec{PayloadCapture: &mgv1.PolicyPayloadCapture{Disabled: true}},
	}
	if err := e.c.Create(context.Background(), pol); err != nil {
		t.Fatal(err)
	}
	e.update(func(mr *mgv1.ModelRollout) {
		mr.Spec.Capture.Payloads = &mgv1.PayloadCaptureSpec{Until: metav1.NewTime(e.clock.Add(time.Hour))}
	})
	e.reconcile()
	if _, ok := e.sidecarEnv(newRev)["MODELGATE_PAYLOAD_UNTIL"]; ok {
		t.Error("payload capture reached the sidecar despite the policy")
	}
	if c := findCondition(e.get(), ConditionPolicyApplied); c == nil || c.Reason != "Tightened" {
		t.Errorf("PolicyApplied %+v", c)
	}
}
