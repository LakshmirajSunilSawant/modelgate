package v1alpha1

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// OutputType says how to read and compare predictions.
// +kubebuilder:validation:Enum=classification;regression;score
type OutputType string

const (
	OutputClassification OutputType = "classification"
	OutputRegression     OutputType = "regression"
	OutputScore          OutputType = "score"
)

// RouteRef points at the Gateway API HTTPRoute whose backends ModelGate steers.
// ModelGate owns the backendRefs and RequestMirror filters of every rule in
// that route; everything else in the route is left alone.
type RouteRef struct {
	Name string `json:"name"`
}

// Range bounds valid score values; outputs outside it count as invalid.
type Range struct {
	// +optional
	Min *float64 `json:"min,omitempty"`
	// +optional
	Max *float64 `json:"max,omitempty"`
}

// OutputSpec says where predictions are in the model's JSON response.
type OutputSpec struct {
	Type OutputType `json:"type"`
	// JSONPath subset ($.a.b[0]) to the predicted class.
	// +optional
	LabelPath string `json:"labelPath,omitempty"`
	// JSONPath subset to the score, confidence, or regression value.
	// +optional
	ScorePath string `json:"scorePath,omitempty"`
	// +optional
	ScoreRange *Range `json:"scoreRange,omitempty"`
}

// ShadowSpec mirrors live traffic to the canary without exposing its
// responses to users.
type ShadowSpec struct {
	Duration metav1.Duration `json:"duration"`
	// Share of requests mirrored to the canary, 0 to 1.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=1
	// +optional
	SampleRate float64 `json:"sampleRate,omitempty"`
}

// Step sends a share of live traffic to the canary for at least Pause.
type Step struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	Weight int32           `json:"weight"`
	Pause  metav1.Duration `json:"pause"`
}

// StrategySpec is the sequence of stages a rollout goes through.
// +kubebuilder:validation:XValidation:rule="size(self.steps) == 0 || self.steps[0].weight <= 50",message="the first step may send at most 50% of traffic to the canary"
// +kubebuilder:validation:XValidation:rule="has(self.shadow) || size(self.steps) > 0",message="at least one of shadow or steps is required"
type StrategySpec struct {
	// +optional
	Shadow *ShadowSpec `json:"shadow,omitempty"`
	// +optional
	// +kubebuilder:validation:MaxItems=20
	Steps []Step `json:"steps,omitempty"`
}

// CheckSpec configures one quality check.
type CheckSpec struct {
	Name string `json:"name"`
	// labelAccuracy needs ground-truth labels posted to the analyzer's
	// /v1/outcomes endpoint; its max is the allowed accuracy drop.
	// +kubebuilder:validation:Enum=agreementRate;meanAbsDiff;psi;chiSquare;ks;wasserstein;validity;p99Ratio;errorRate;labelAccuracy
	Type string `json:"type"`
	// +optional
	Min *float64 `json:"min,omitempty"`
	// +optional
	Max *float64 `json:"max,omitempty"`
	// Restrict the check to one phase. Empty means both.
	// +kubebuilder:validation:Enum=shadow;canary
	// +optional
	Phase string `json:"phase,omitempty"`
	// Samples this check needs from each version, overriding
	// analysis.minSamples. labelAccuracy defaults to 200 labeled predictions.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinSamples *int64 `json:"minSamples,omitempty"`
}

// AnalysisSpec configures how verdicts are reached.
type AnalysisSpec struct {
	// Samples needed from each version before statistical checks are judged.
	// +kubebuilder:default=2000
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinSamples int64 `json:"minSamples,omitempty"`
	// Consecutive failed evaluations that roll the canary back.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	FailureLimit int32 `json:"failureLimit,omitempty"`
	// False-alarm budget per check per stage.
	// +kubebuilder:default=0.01
	// +optional
	Alpha float64 `json:"alpha,omitempty"`
	// How often the controller asks the analyzer for a verdict.
	// +kubebuilder:default="30s"
	// +optional
	Interval metav1.Duration `json:"interval,omitempty"`
	// +kubebuilder:validation:MinItems=1
	Checks []CheckSpec `json:"checks"`
}

// CaptureSpec configures the injected capture sidecar.
type CaptureSpec struct {
	// Share of requests whose predictions are recorded, 0 to 1.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=1
	// +optional
	SampleRate float64 `json:"sampleRate,omitempty"`
	// Payloads records redacted request and response bodies for debugging.
	// It is off unless set, and it switches itself off at payloads.until.
	// +optional
	Payloads *PayloadCaptureSpec `json:"payloads,omitempty"`
}

// MaxPayloadCaptureWindow is how far ahead payloads.until may be.
const MaxPayloadCaptureWindow = 24 * time.Hour

// PayloadCaptureSpec turns on raw payload capture for a limited time. Only
// JSON bodies are kept, so that every redact path can be applied; other
// bodies, and bodies larger than maxBytes, are left out. Headers are never
// captured. Redaction happens in the sidecar, before anything leaves the pod.
type PayloadCaptureSpec struct {
	// Capture stops at this time. It must be at most 24 hours ahead;
	// otherwise capture stays off and the PayloadCapture condition says why.
	Until metav1.Time `json:"until"`
	// Share of requests whose bodies are kept. Only requests that are also
	// captured for analysis qualify, and the same ones are picked on stable
	// and canary, so both responses to one request can be compared.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	// +kubebuilder:default=0.01
	// +optional
	SampleRate float64 `json:"sampleRate,omitempty"`
	// Largest body kept, in bytes.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65536
	// +kubebuilder:default=8192
	// +optional
	MaxBytes int32 `json:"maxBytes,omitempty"`
	// JSONPath fields replaced with "[REDACTED]" in request and response
	// bodies, such as $.instances[*].ssn or $.patient.*. Supports .key,
	// ['key'], [n], [*] and .*.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	Redact []string `json:"redact,omitempty"`
}

// ModelRolloutSpec defines the desired model deployment and how new
// versions are rolled out.
type ModelRolloutSpec struct {
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
	// Pod template for the model server, as in a Deployment. Changing it
	// starts a rollout.
	Template corev1.PodTemplateSpec `json:"template"`
	// Container port the model server listens on inside the pod.
	// +kubebuilder:default=8080
	// +optional
	ModelPort int32        `json:"modelPort,omitempty"`
	RouteRef  RouteRef     `json:"routeRef"`
	Output    OutputSpec   `json:"output"`
	Strategy  StrategySpec `json:"strategy"`
	Analysis  AnalysisSpec `json:"analysis"`
	// +optional
	Capture CaptureSpec `json:"capture,omitempty"`
	// Paused freezes the rollout at its current weight.
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// Phase is the coarse rollout state.
type Phase string

const (
	PhaseHealthy     Phase = "Healthy"     // stable serving all traffic, nothing in flight
	PhaseProgressing Phase = "Progressing" // canary under analysis
	PhasePaused      Phase = "Paused"      // held by user, kill switch, or missing evidence
	PhaseDegraded    Phase = "Degraded"    // last canary was rolled back
)

// Condition types, compatible with kstatus.
const (
	ConditionReady       = "Ready"
	ConditionReconciling = "Reconciling"
	ConditionStalled     = "Stalled"
	// ConditionPayloadCapture reports whether raw payload capture is on.
	ConditionPayloadCapture = "PayloadCapture"
)

// ShadowStepIndex is CurrentStep during the shadow stage.
const ShadowStepIndex = -1

// CheckStatus is one check's latest result.
type CheckStatus struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Value     string `json:"value,omitempty"`
	Threshold string `json:"threshold,omitempty"`
	Message   string `json:"message,omitempty"`
}

// PredictionSample is one version's prediction on a sampled request.
type PredictionSample struct {
	// +optional
	Label string `json:"label,omitempty"`
	// +optional
	Score string `json:"score,omitempty"`
}

// DisagreementSample is a request on which stable and canary disagreed.
type DisagreementSample struct {
	RequestID string           `json:"requestId"`
	Stable    PredictionSample `json:"stable"`
	Canary    PredictionSample `json:"canary"`
}

// VerdictStatus summarizes the latest analyzer verdict.
type VerdictStatus struct {
	Decision      string      `json:"decision"`
	Reason        string      `json:"reason,omitempty"`
	Time          metav1.Time `json:"time"`
	Look          int32       `json:"look,omitempty"`
	StableSamples int64       `json:"stableSamples"`
	CanarySamples int64       `json:"canarySamples"`
	PairedSamples int64       `json:"pairedSamples,omitempty"`
	// +optional
	Checks []CheckStatus `json:"checks,omitempty"`
	// +optional
	Disagreements []DisagreementSample `json:"disagreements,omitempty"`
}

// ModelRolloutStatus is the observed state. The controller resumes from it
// after a crash, so it records everything needed to continue.
type ModelRolloutStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	Phase Phase `json:"phase,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// Revision serving production traffic.
	// +optional
	StableRevision string `json:"stableRevision,omitempty"`
	// Revision under analysis, empty when nothing is in flight.
	// +optional
	CanaryRevision string `json:"canaryRevision,omitempty"`
	// Last revision that was rolled back; not retried until the template changes.
	// +optional
	AbortedRevision string `json:"abortedRevision,omitempty"`
	// -1 for shadow, otherwise the index into spec.strategy.steps.
	// +optional
	CurrentStep int32 `json:"currentStep"`
	// +optional
	CanaryWeight int32 `json:"canaryWeight"`
	// +optional
	StepStartedAt *metav1.Time `json:"stepStartedAt,omitempty"`
	// +optional
	RolloutStartedAt *metav1.Time `json:"rolloutStartedAt,omitempty"`
	// +optional
	Looks int32 `json:"looks,omitempty"`
	// +optional
	ConsecutiveFailures int32 `json:"consecutiveFailures,omitempty"`
	// +optional
	LastVerdict *VerdictStatus `json:"lastVerdict,omitempty"`
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mr
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Stable",type=string,JSONPath=`.status.stableRevision`
// +kubebuilder:printcolumn:name="Canary",type=string,JSONPath=`.status.canaryRevision`
// +kubebuilder:printcolumn:name="Weight",type=integer,JSONPath=`.status.canaryWeight`
// +kubebuilder:printcolumn:name="Verdict",type=string,JSONPath=`.status.lastVerdict.decision`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelRollout replaces a model's Deployment and rolls out new versions
// behind a model-quality gate.
type ModelRollout struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelRolloutSpec   `json:"spec,omitempty"`
	Status ModelRolloutStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ModelRolloutList is a list of ModelRollouts.
type ModelRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ModelRollout{}, &ModelRolloutList{})
}

// Annotations for manual control. The kubectl plugin sets them; the
// controller acts on them once and removes ActionAnnotation.
const (
	ActionAnnotation   = "modelgate.dev/action" // pause | resume | abort | promote
	ActionByAnnotation = "modelgate.dev/action-by"
)
