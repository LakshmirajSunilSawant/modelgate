package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyName is the only name a ModelGatePolicy may have: one per namespace.
const PolicyName = "default"

// ModelGatePolicySpec is the floor every ModelRollout in the namespace must
// meet. Rollouts may be stricter, never looser. The controller enforces it
// at runtime by merging it into each rollout, and the validating webhook
// rejects rollouts that ask for less.
type ModelGatePolicySpec struct {
	// Checks every rollout gets. A rollout check of the same type covering
	// the same phase may set a stricter threshold; a missing one is added.
	// +optional
	Checks []CheckSpec `json:"checks,omitempty"`
	// Rollouts must use at least this many samples before judging.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinSamples *int64 `json:"minSamples,omitempty"`
	// Rollouts may allow at most this many consecutive failed evaluations.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxFailureLimit *int32 `json:"maxFailureLimit,omitempty"`
	// Rollouts may use at most this false-alarm budget per check per stage.
	// +optional
	MaxAlpha *float64 `json:"maxAlpha,omitempty"`
	// The first canary step may send at most this share of traffic.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +optional
	MaxFirstStepWeight *int32 `json:"maxFirstStepWeight,omitempty"`
	// Rules for raw payload capture (spec.capture.payloads).
	// +optional
	PayloadCapture *PolicyPayloadCapture `json:"payloadCapture,omitempty"`
}

// PolicyPayloadCapture limits raw payload capture in a namespace.
type PolicyPayloadCapture struct {
	// Disabled forbids payload capture for every rollout in the namespace.
	// +optional
	Disabled bool `json:"disabled,omitempty"`
	// Fields redacted on top of each rollout's own redact list.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	Redact []string `json:"redact,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:shortName=mgp
// +kubebuilder:validation:XValidation:rule="self.metadata.name == 'default'",message="a namespace has one ModelGatePolicy, named default"

// ModelGatePolicy sets namespace-wide quality-gate defaults that teams can
// only tighten.
type ModelGatePolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec ModelGatePolicySpec `json:"spec,omitempty"`
}

// +kubebuilder:object:root=true

// ModelGatePolicyList is a list of ModelGatePolicies.
type ModelGatePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelGatePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ModelGatePolicy{}, &ModelGatePolicyList{})
}
