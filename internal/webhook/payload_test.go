package webhook

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

func TestValidatePayloads(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	mr := func(ahead time.Duration, redact ...string) *mgv1.ModelRollout {
		m := &mgv1.ModelRollout{}
		m.Spec.Capture.Payloads = &mgv1.PayloadCaptureSpec{Until: metav1.NewTime(now.Add(ahead)), Redact: redact}
		return m
	}
	if err := validatePayloads(&mgv1.ModelRollout{}, now); err != nil {
		t.Errorf("no payloads: %v", err)
	}
	if err := validatePayloads(mr(2*time.Hour, "$.ssn", "$.rows[*].name"), now); err != nil {
		t.Errorf("valid payloads rejected: %v", err)
	}
	if err := validatePayloads(mr(-time.Hour), now); err != nil {
		t.Errorf("an ended capture must not block other spec changes: %v", err)
	}
	if validatePayloads(mr(25*time.Hour), now) == nil {
		t.Error("until 25h ahead accepted")
	}
	if validatePayloads(mr(time.Hour, "ssn"), now) == nil {
		t.Error("bad redact path accepted")
	}
}
