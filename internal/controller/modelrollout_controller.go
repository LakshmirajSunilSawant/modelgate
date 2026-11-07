// Package controller reconciles ModelRollouts: it runs stable and canary
// Deployments, steers the HTTPRoute, asks the analyzer for verdicts, and
// promotes or rolls back.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/analysis"
	"github.com/modelgate/modelgate/internal/audit"
	"github.com/modelgate/modelgate/internal/capture"
	"github.com/modelgate/modelgate/internal/policy"
)

const (
	// KillSwitchKey in the kill-switch ConfigMap freezes every rollout when "true".
	KillSwitchKey     = "freeze"
	templateHashAnnot = "modelgate.dev/template-hash"
	// ConditionPolicyApplied reports how the namespace ModelGatePolicy changed the rollout.
	ConditionPolicyApplied = "PolicyApplied"
	defaultInterval        = 30 * time.Second
	waitForPodsInterval    = 10 * time.Second
)

// ModelRolloutReconciler reconciles ModelRollout objects.
type ModelRolloutReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Recorder   record.EventRecorder
	Analyzer   Analyzer
	Sidecar    SidecarConfig
	KillSwitch types.NamespacedName
	// APIReader reads the kill switch without a cache, so the controller
	// does not need to watch every ConfigMap in the cluster. Defaults to Client.
	APIReader client.Reader
	Now       func() time.Time
	// Audit stores every decision durably. Nil disables the audit log.
	Audit audit.Log

	// journal collects this pass's audit entries. Reconcile works on a
	// shallow copy of the reconciler, so each pass has its own journal.
	journal *[]audit.Entry
}

const pausedByUserMsg = "rollout paused (spec.paused)"

// +kubebuilder:rbac:groups=modelgate.dev,resources=modelrollouts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=modelgate.dev,resources=modelrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=modelgate.dev,resources=modelgatepolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=gateway.networking.k8s.io,resources=httproutes,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete

// Reconcile runs one pass. The pass is structured so a crash at any point
// is safe: decisions are written to status first, then resources are
// converged to match status. Converging is idempotent.
func (r *ModelRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pass := *r
	pass.journal = &[]audit.Entry{}
	return pass.reconcile(ctx, req)
}

func (r *ModelRolloutReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mr := &mgv1.ModelRollout{}
	if err := r.Get(ctx, req.NamespacedName, mr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !mr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // owned objects are garbage collected
	}
	rev := Revision(&mr.Spec.Template)
	logger := log.FromContext(ctx).WithValues("rollout", rolloutKey(mr), "revision", rev)
	ctx = log.IntoContext(ctx, logger)

	// Phase 1: transitions driven by the spec and manual actions.
	before := mr.Status.DeepCopy()
	// The namespace policy is merged into the in-memory spec before anything
	// reads it. The spec is never written back, only status and annotations.
	pol, err := r.applyPolicy(ctx, mr)
	if err != nil {
		return ctrl.Result{}, err
	}
	r.applyPayloadCapture(mr)
	r.applyReplay(ctx, mr)
	r.applySpec(mr, rev)
	action := r.applyAction(mr)
	if err := r.saveStatus(ctx, mr, before, pol); err != nil {
		return ctrl.Result{}, err
	}
	if action != "" {
		patch := client.MergeFrom(mr.DeepCopy())
		delete(mr.Annotations, mgv1.ActionAnnotation)
		if err := r.Patch(ctx, mr, patch); err != nil {
			return ctrl.Result{}, err
		}
		policy.Apply(pol, &mr.Spec) // the patch response carries the stored spec
	}

	// Converge resources to what status says.
	if err := r.converge(ctx, mr, rev); err != nil {
		before = mr.Status.DeepCopy()
		setCondition(mr, mgv1.ConditionStalled, metav1.ConditionTrue, "ConvergeFailed", err.Error())
		_ = r.saveStatus(ctx, mr, before, pol)
		return ctrl.Result{}, err
	}

	// Phase 2: analysis-driven transitions.
	before = mr.Status.DeepCopy()
	result := r.analyze(ctx, mr)
	r.setReadiness(mr)
	recordMetrics(mr)
	if err := r.saveStatus(ctx, mr, before, pol); err != nil {
		return ctrl.Result{}, err
	}
	if before.StableRevision != mr.Status.StableRevision || before.CanaryRevision != mr.Status.CanaryRevision ||
		before.CurrentStep != mr.Status.CurrentStep {
		if err := r.converge(ctx, mr, rev); err != nil {
			return ctrl.Result{}, err
		}
	}
	// Come back when payload capture ends, so the condition says so on time.
	if c := meta.FindStatusCondition(mr.Status.Conditions, mgv1.ConditionPayloadCapture); c != nil && c.Reason == payloadActive {
		if left := mr.Spec.Capture.Payloads.Until.Sub(r.now()); left > 0 && (result.RequeueAfter == 0 || left < result.RequeueAfter) {
			result.RequeueAfter = left + time.Second
		}
	}
	return result, nil
}

// Reasons of the PayloadCapture condition.
const (
	payloadActive    = "Active"
	payloadExpired   = "Expired"
	payloadPast      = "UntilInPast"
	payloadTooLong   = "UntilTooFar"
	payloadBadConfig = "InvalidRedactPath"
)

// applyPayloadCapture records in the PayloadCapture condition whether raw
// payload capture may run. The sidecar is configured for capture only when
// the condition allows it, and the sidecar itself stops at the deadline. An
// expired capture keeps its sidecar settings, so expiry restarts no pods.
func (r *ModelRolloutReconciler) applyPayloadCapture(mr *mgv1.ModelRollout) {
	pc := mr.Spec.Capture.Payloads
	if pc == nil {
		meta.RemoveStatusCondition(&mr.Status.Conditions, mgv1.ConditionPayloadCapture)
		return
	}
	until := pc.Until.UTC().Format(time.RFC3339)
	off := func(reason, msg string) {
		setCondition(mr, mgv1.ConditionPayloadCapture, metav1.ConditionFalse, reason, msg)
	}
	left := pc.Until.Sub(r.now())
	_, pathErr := capture.ParseRedactPaths(pc.Redact)
	switch {
	case pathErr != nil:
		off(payloadBadConfig, "payload capture is off: "+pathErr.Error())
	case left <= 0 && payloadCaptureAllowed(mr):
		off(payloadExpired, "payload capture ended at "+until)
	case left <= 0:
		off(payloadPast, "payload capture is off: until "+until+" has passed")
	case left > mgv1.MaxPayloadCaptureWindow:
		off(payloadTooLong, fmt.Sprintf("payload capture is off: until must be at most %s ahead", mgv1.MaxPayloadCaptureWindow))
	default:
		setCondition(mr, mgv1.ConditionPayloadCapture, metav1.ConditionTrue, payloadActive, "capturing redacted payloads until "+until)
	}
}

// payloadCaptureAllowed reports whether the sidecar gets the payload
// capture settings. Expired settings stay, so nothing restarts at expiry.
func payloadCaptureAllowed(mr *mgv1.ModelRollout) bool {
	if mr.Spec.Capture.Payloads == nil {
		return false
	}
	c := meta.FindStatusCondition(mr.Status.Conditions, mgv1.ConditionPayloadCapture)
	return c != nil && (c.Reason == payloadActive || c.Reason == payloadExpired)
}

func (r *ModelRolloutReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// saveStatus writes status if it changed. The update response carries the
// stored spec, so the namespace policy is merged in again afterwards.
func (r *ModelRolloutReconciler) saveStatus(ctx context.Context, mr *mgv1.ModelRollout, before *mgv1.ModelRolloutStatus, pol *mgv1.ModelGatePolicySpec) error {
	mr.Status.ObservedGeneration = mr.Generation
	if err := r.flushAudit(ctx, mr); err != nil {
		return err
	}
	if equality.Semantic.DeepEqual(*before, mr.Status) {
		return nil
	}
	err := r.Status().Update(ctx, mr)
	policy.Apply(pol, &mr.Spec)
	return err
}

// applySpec handles first deploys, new templates, and reverted templates.
func (r *ModelRolloutReconciler) applySpec(mr *mgv1.ModelRollout, rev string) {
	st := &mr.Status
	switch {
	case st.StableRevision == "":
		st.StableRevision = rev
		st.Phase = mgv1.PhaseHealthy
		st.Message = "initial revision deployed"
		r.Recorder.Eventf(mr, corev1.EventTypeNormal, "Deployed", "revision %s deployed as stable", rev)
		r.record(mr, audit.Entry{Action: audit.ActionDeployed, Key: rev, Reason: st.Message})
	case rev == st.StableRevision && st.CanaryRevision != "":
		r.Recorder.Eventf(mr, corev1.EventTypeNormal, "CanaryCancelled", "template reverted to stable %s; dropping canary %s", rev, st.CanaryRevision)
		r.record(mr, audit.Entry{Action: audit.ActionCanaryCancelled, Key: st.CanaryRevision, Reason: "template reverted to stable revision"})
		clearCanary(st)
		st.Phase = mgv1.PhaseHealthy
		st.Message = "template reverted to stable revision"
	case rev == st.StableRevision:
		if st.Phase == "" {
			st.Phase = mgv1.PhaseHealthy
		}
	case rev == st.AbortedRevision:
		// Rolled back already; wait for a new template.
	case rev != st.CanaryRevision:
		if st.CanaryRevision != "" {
			r.Recorder.Eventf(mr, corev1.EventTypeNormal, "CanaryReplaced", "template changed mid-rollout; replacing canary %s with %s", st.CanaryRevision, rev)
			r.record(mr, audit.Entry{Action: audit.ActionCanaryReplaced, Key: st.CanaryRevision + ">" + rev, Reason: "replaced by " + rev})
		}
		now := metav1.NewTime(r.now())
		clearCanary(st)
		st.LastVerdict = nil // the previous rollout's verdict lives on in events
		st.CanaryRevision = rev
		st.RolloutStartedAt = &now
		st.StepStartedAt = &now
		st.Phase = mgv1.PhaseProgressing
		if mr.Spec.Strategy.Shadow != nil {
			st.CurrentStep = mgv1.ShadowStepIndex
			st.Message = "shadow analysis started"
		} else {
			st.CurrentStep = 0
			st.CanaryWeight = mr.Spec.Strategy.Steps[0].Weight
			st.Message = fmt.Sprintf("step 1 started at %d%%", st.CanaryWeight)
		}
		r.Recorder.Eventf(mr, corev1.EventTypeNormal, "RolloutStarted", "canary %s against stable %s", rev, st.StableRevision)
		r.record(mr, audit.Entry{Action: audit.ActionRolloutStarted, Key: rev, Reason: st.Message, Blocking: true})
	}
}

// applyAction handles abort and promote annotations. Pause and resume are
// spec.paused, which the kubectl plugin sets directly.
func (r *ModelRolloutReconciler) applyAction(mr *mgv1.ModelRollout) string {
	action := mr.Annotations[mgv1.ActionAnnotation]
	if action == "" {
		return ""
	}
	by := mr.Annotations[mgv1.ActionByAnnotation]
	if by == "" {
		by = "unknown"
	}
	st := &mr.Status
	switch action {
	case "abort":
		if st.CanaryRevision != "" {
			r.rollback(mr, by, fmt.Sprintf("aborted by %s", by))
		}
	case "promote":
		if st.CanaryRevision != "" {
			r.Recorder.Eventf(mr, corev1.EventTypeWarning, "ForcePromoted", "canary %s promoted by %s without completing analysis", st.CanaryRevision, by)
			r.record(mr, audit.Entry{Action: audit.ActionForcePromoted, Actor: by, Key: st.CanaryRevision,
				Reason: "promoted without completing analysis", Blocking: true})
			r.promote(mr, by, fmt.Sprintf("force-promoted by %s", by))
		}
	default:
		r.Recorder.Eventf(mr, corev1.EventTypeWarning, "UnknownAction", "ignoring %s=%q from %s", mgv1.ActionAnnotation, action, by)
	}
	return action
}

func clearCanary(st *mgv1.ModelRolloutStatus) {
	st.CanaryRevision = ""
	st.CurrentStep = 0
	st.CanaryWeight = 0
	st.StepStartedAt = nil
	st.Looks = 0
	st.ConsecutiveFailures = 0
}

func (r *ModelRolloutReconciler) promote(mr *mgv1.ModelRollout, actor, why string) {
	st := &mr.Status
	rev := st.CanaryRevision
	r.record(mr, audit.Entry{Action: audit.ActionPromoted, Actor: actor, Key: rev, Reason: why, Blocking: true})
	st.StableRevision = rev
	clearCanary(st)
	st.Phase = mgv1.PhaseHealthy
	st.Message = fmt.Sprintf("revision %s promoted: %s", rev, why)
	r.Recorder.Eventf(mr, corev1.EventTypeNormal, "Promoted", "revision %s promoted to stable: %s", rev, why)
}

func (r *ModelRolloutReconciler) rollback(mr *mgv1.ModelRollout, actor, why string) {
	st := &mr.Status
	rev := st.CanaryRevision
	r.record(mr, audit.Entry{Action: audit.ActionRolledBack, Actor: actor, Key: rev, Reason: why})
	st.AbortedRevision = rev
	clearCanary(st)
	st.Phase = mgv1.PhaseDegraded
	st.Message = fmt.Sprintf("revision %s rolled back: %s", rev, why)
	r.Recorder.Eventf(mr, corev1.EventTypeWarning, "RolledBack", "revision %s rolled back to %s: %s", rev, st.StableRevision, why)
	decisions.WithLabelValues(mr.Namespace, mr.Name, "rollback").Inc()
}

// converge makes Deployments, Services, and the HTTPRoute match status.
func (r *ModelRolloutReconciler) converge(ctx context.Context, mr *mgv1.ModelRollout, rev string) error {
	st := mr.Status
	if err := r.ensureDeployment(ctx, mr, st.StableRevision, st.StableRevision == rev); err != nil {
		return err
	}
	if st.CanaryRevision != "" {
		if err := r.ensureDeployment(ctx, mr, st.CanaryRevision, st.CanaryRevision == rev); err != nil {
			return err
		}
	}
	canaryTarget := st.CanaryRevision
	if canaryTarget == "" {
		canaryTarget = st.StableRevision
	}
	if err := r.ensureService(ctx, mr, stableServiceName(mr), st.StableRevision); err != nil {
		return err
	}
	if err := r.ensureService(ctx, mr, canaryServiceName(mr), canaryTarget); err != nil {
		return err
	}
	if err := r.ensureRoute(ctx, mr); err != nil {
		return err
	}
	// Remove old revisions only after traffic has moved off them.
	return r.cleanup(ctx, mr, st.StableRevision, st.CanaryRevision)
}

// sidecarHash identifies the rendered capture container, so changes to
// spec.output, spec.capture, or the sidecar flags reach running Deployments.
func (r *ModelRolloutReconciler) sidecarHash(mr *mgv1.ModelRollout, rev string) string {
	return rev + "-" + hashJSON(buildSidecar(mr, rev, r.Sidecar))
}

// ensureDeployment creates or updates one revision's Deployment. Only the
// current template can be rendered; an older stable revision is kept as it
// is, with replicas synced.
func (r *ModelRolloutReconciler) ensureDeployment(ctx context.Context, mr *mgv1.ModelRollout, rev string, fromTemplate bool) error {
	d := &appsv1.Deployment{}
	key := types.NamespacedName{Namespace: mr.Namespace, Name: deploymentName(mr, rev)}
	err := r.Get(ctx, key, d)
	if apierrors.IsNotFound(err) {
		if !fromTemplate {
			return fmt.Errorf("deployment %s for stable revision %s is missing and its template is no longer in the spec", key.Name, rev)
		}
		desired := buildDeployment(mr, rev, r.Sidecar)
		desired.Annotations = map[string]string{templateHashAnnot: r.sidecarHash(mr, rev)}
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	patch := client.MergeFrom(d.DeepCopy())
	d.Spec.Replicas = ptr(replicas(mr))
	if want := r.sidecarHash(mr, rev); d.Annotations[templateHashAnnot] != want {
		if fromTemplate {
			d.Spec.Template = buildDeployment(mr, rev, r.Sidecar).Spec.Template
		} else {
			// An older stable revision cannot be re-rendered from the spec, but
			// its sidecar can: keep capture settings identical on both sides.
			containers := withoutContainer(d.Spec.Template.Spec.Containers, sidecarName)
			d.Spec.Template.Spec.Containers = append(containers, buildSidecar(mr, rev, r.Sidecar))
		}
		if d.Annotations == nil {
			d.Annotations = map[string]string{}
		}
		d.Annotations[templateHashAnnot] = want
	}
	return r.Patch(ctx, d, patch)
}

func (r *ModelRolloutReconciler) ensureService(ctx context.Context, mr *mgv1.ModelRollout, name, rev string) error {
	desired := buildService(mr, name, rev)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: mr.Namespace}}
	_, err := controllerutil.CreateOrPatch(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(mr, svc, r.Scheme)
	})
	return err
}

func (r *ModelRolloutReconciler) ensureRoute(ctx context.Context, mr *mgv1.ModelRollout) error {
	route := &gwv1.HTTPRoute{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mr.Spec.RouteRef.Name}, route); err != nil {
		return fmt.Errorf("getting HTTPRoute %s: %w", mr.Spec.RouteRef.Name, err)
	}
	if !applyPlan(route, mr, planFor(mr)) {
		return nil
	}
	return r.Update(ctx, route)
}

func (r *ModelRolloutReconciler) cleanup(ctx context.Context, mr *mgv1.ModelRollout, keep ...string) error {
	var list appsv1.DeploymentList
	if err := r.List(ctx, &list, client.InNamespace(mr.Namespace), client.MatchingLabels{labelRollout: mr.Name}); err != nil {
		return err
	}
	for i := range list.Items {
		d := &list.Items[i]
		if !metav1.IsControlledBy(d, mr) || contains(keep, d.Labels[labelRevision]) {
			continue
		}
		if err := r.Delete(ctx, d); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x != "" && x == s {
			return true
		}
	}
	return false
}

// frozen reports whether the cluster-wide kill switch is on. If the switch
// cannot be read, rollouts freeze: no promotion without certainty.
func (r *ModelRolloutReconciler) frozen(ctx context.Context) (bool, string) {
	if r.KillSwitch.Name == "" {
		return false, ""
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	cm := &corev1.ConfigMap{}
	err := reader.Get(ctx, r.KillSwitch, cm)
	switch {
	case apierrors.IsNotFound(err):
		return false, ""
	case err != nil:
		return true, "kill switch unreadable: " + err.Error()
	}
	if on, _ := strconv.ParseBool(cm.Data[KillSwitchKey]); on {
		return true, fmt.Sprintf("kill switch %s is on", r.KillSwitch)
	}
	return false, ""
}

func (r *ModelRolloutReconciler) interval(mr *mgv1.ModelRollout) time.Duration {
	if d := mr.Spec.Analysis.Interval.Duration; d > 0 {
		return d
	}
	return defaultInterval
}

// analyze asks for a verdict and applies the decision rules.
func (r *ModelRolloutReconciler) analyze(ctx context.Context, mr *mgv1.ModelRollout) ctrl.Result {
	st := &mr.Status
	if st.CanaryRevision == "" {
		return ctrl.Result{}
	}
	interval := r.interval(mr)
	logger := log.FromContext(ctx)

	canary := &appsv1.Deployment{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: deploymentName(mr, st.CanaryRevision)}, canary); err != nil || !deploymentAvailable(canary) {
		st.Message = "waiting for canary pods to become available"
		// The analysis clock starts once the canary can actually serve.
		now := metav1.NewTime(r.now())
		st.StepStartedAt = &now
		return ctrl.Result{RequeueAfter: waitForPodsInterval}
	}
	if mr.Spec.Paused {
		r.hold(mr, "PausedByUser", pausedByUserMsg)
		return ctrl.Result{RequeueAfter: interval}
	}
	if st.Phase == mgv1.PhasePaused && st.Message == pausedByUserMsg {
		st.Phase = mgv1.PhaseProgressing
		st.Message = "resumed"
		r.record(mr, audit.Entry{Action: audit.ActionResumed, Actor: actionBy(mr), Reason: "spec.paused cleared"})
	}
	if on, why := r.frozen(ctx); on {
		r.hold(mr, "KillSwitch", why)
		return ctrl.Result{RequeueAfter: interval}
	}
	// Watch events (Deployment status, our own status writes) trigger extra
	// reconciles. Looks are spaced by the interval so failureLimit counts
	// evaluations of new evidence, not back-to-back reads of the same data.
	if v := st.LastVerdict; v != nil && st.Looks > 0 {
		if wait := interval - r.now().Sub(v.Time.Time); wait > 0 {
			return ctrl.Result{RequeueAfter: wait}
		}
	}

	phase := analysis.PhaseCanary
	if st.CurrentStep == mgv1.ShadowStepIndex {
		phase = analysis.PhaseShadow
	}
	req := analysis.Request{
		Rollout: rolloutKey(mr),
		Stable:  st.StableRevision,
		Canary:  st.CanaryRevision,
		Since:   st.StepStartedAt.Time,
		// labelAccuracy pools labels over the whole rollout.
		RolloutStart: rolloutStart(st),
		Phase:        phase,
		Look:         int(st.Looks) + 1,
		MinSamples:   mr.Spec.Analysis.MinSamples,
		Alpha:        mr.Spec.Analysis.Alpha,
	}
	for _, c := range mr.Spec.Analysis.Checks {
		req.Checks = append(req.Checks, analysis.CheckSpec{Name: c.Name, Type: c.Type, Min: c.Min, Max: c.Max, Phase: c.Phase, MinSamples: c.MinSamples})
	}
	v, err := r.Analyzer.Verdict(ctx, req)
	if err != nil {
		logger.Error(err, "analyzer unavailable; holding rollout")
		r.hold(mr, "AnalyzerUnavailable", "analyzer unavailable, holding at current weight: "+err.Error())
		return ctrl.Result{RequeueAfter: interval}
	}
	st.Looks++
	st.LastVerdict = verdictStatus(v, st.StableRevision, st.CanaryRevision, r.now())
	verdictJSON, _ := json.Marshal(st.LastVerdict)
	r.record(mr, audit.Entry{Action: audit.ActionVerdict, Key: fmt.Sprintf("%d/%d", st.CurrentStep, st.Looks),
		Reason: v.Decision + ": " + v.Reason, Verdict: verdictJSON})
	decisions.WithLabelValues(mr.Namespace, mr.Name, v.Decision).Inc()
	for _, c := range v.Checks {
		if c.Status == analysis.Pass || c.Status == analysis.Fail {
			checkValue.WithLabelValues(mr.Namespace, mr.Name, c.Name).Set(c.Value)
		}
	}
	logger.Info("verdict", "phase", phase, "look", v.Look, "decision", v.Decision, "reason", v.Reason)
	st.Phase = mgv1.PhaseProgressing
	meta.RemoveStatusCondition(&st.Conditions, mgv1.ConditionStalled)

	limit := mr.Spec.Analysis.FailureLimit
	if limit <= 0 {
		limit = 2
	}
	switch v.Decision {
	case analysis.Fail:
		st.ConsecutiveFailures++
		r.Recorder.Eventf(mr, corev1.EventTypeWarning, "CheckFailed", "%s (failure %d/%d)", v.Reason, st.ConsecutiveFailures, limit)
		if v.HardFailure {
			r.rollback(mr, audit.ControllerActor, "hard failure: "+v.Reason)
		} else if st.ConsecutiveFailures >= limit {
			r.rollback(mr, audit.ControllerActor, v.Reason)
		} else {
			st.Message = fmt.Sprintf("check failed (%d/%d): %s", st.ConsecutiveFailures, limit, v.Reason)
		}
		return ctrl.Result{RequeueAfter: interval}
	case analysis.Inconclusive:
		st.Message = stageName(mr) + ": " + v.Reason
		if r.now().Sub(st.StepStartedAt.Time) >= r.stagePause(mr) {
			st.Message = stageName(mr) + ": stage time elapsed, holding for evidence: " + v.Reason
		}
		return ctrl.Result{RequeueAfter: interval}
	}

	st.ConsecutiveFailures = 0
	if r.now().Sub(st.StepStartedAt.Time) < r.stagePause(mr) {
		st.Message = stageName(mr) + ": passing, stage time remaining"
		return ctrl.Result{RequeueAfter: interval}
	}
	// Promotions and step changes are only made when they can be audited.
	if err := r.auditLog().Ping(ctx); err != nil {
		logger.Error(err, "audit log unavailable; holding rollout")
		r.hold(mr, "AuditUnavailable", "audit log unavailable, holding at current weight: "+err.Error())
		return ctrl.Result{RequeueAfter: interval}
	}
	r.advance(mr)
	return ctrl.Result{RequeueAfter: time.Second}
}

func (r *ModelRolloutReconciler) hold(mr *mgv1.ModelRollout, reason, msg string) {
	st := &mr.Status
	if st.Phase != mgv1.PhasePaused || st.Message != msg {
		r.Recorder.Event(mr, corev1.EventTypeWarning, reason, msg)
		actor := audit.ControllerActor
		if reason == "PausedByUser" {
			actor = actionBy(mr)
		}
		r.record(mr, audit.Entry{Action: audit.ActionHeld, Actor: actor, Reason: reason + ": " + msg})
	}
	st.Phase = mgv1.PhasePaused
	st.Message = msg
}

func (r *ModelRolloutReconciler) stagePause(mr *mgv1.ModelRollout) time.Duration {
	st := mr.Status
	if st.CurrentStep == mgv1.ShadowStepIndex {
		return mr.Spec.Strategy.Shadow.Duration.Duration
	}
	return mr.Spec.Strategy.Steps[st.CurrentStep].Pause.Duration
}

func stageName(mr *mgv1.ModelRollout) string {
	st := mr.Status
	if st.CurrentStep == mgv1.ShadowStepIndex {
		return "shadow"
	}
	return fmt.Sprintf("step %d/%d (%d%%)", st.CurrentStep+1, len(mr.Spec.Strategy.Steps), st.CanaryWeight)
}

// advance moves to the next stage, or promotes after the last one.
func (r *ModelRolloutReconciler) advance(mr *mgv1.ModelRollout) {
	st := &mr.Status
	next := st.CurrentStep + 1
	if int(next) >= len(mr.Spec.Strategy.Steps) {
		decisions.WithLabelValues(mr.Namespace, mr.Name, "promote").Inc()
		r.promote(mr, audit.ControllerActor, "all analysis stages passed")
		return
	}
	from := stageName(mr)
	now := metav1.NewTime(r.now())
	st.CurrentStep = next
	st.CanaryWeight = mr.Spec.Strategy.Steps[next].Weight
	st.StepStartedAt = &now
	st.Looks = 0
	st.ConsecutiveFailures = 0
	st.Message = stageName(mr) + " started"
	r.Recorder.Eventf(mr, corev1.EventTypeNormal, "StepAdvanced", "%s passed; %s", from, st.Message)
	r.record(mr, audit.Entry{Action: audit.ActionStepAdvanced, Key: fmt.Sprint(next), Reason: from + " passed", Blocking: true})
}

func rolloutStart(st *mgv1.ModelRolloutStatus) time.Time {
	if st.RolloutStartedAt == nil {
		return time.Time{}
	}
	return st.RolloutStartedAt.Time
}

func actionBy(mr *mgv1.ModelRollout) string {
	if by := mr.Annotations[mgv1.ActionByAnnotation]; by != "" {
		return by
	}
	return "unknown"
}

func (r *ModelRolloutReconciler) auditLog() audit.Log {
	if r.Audit == nil {
		return audit.Noop{}
	}
	return r.Audit
}

// record queues an audit entry for this pass. It fills in the rollout's
// identity and the state at the moment of the decision.
func (r *ModelRolloutReconciler) record(mr *mgv1.ModelRollout, e audit.Entry) {
	if r.journal == nil {
		return
	}
	st := mr.Status
	e.Time = r.now()
	e.Namespace, e.Rollout, e.RolloutUID = mr.Namespace, mr.Name, string(mr.UID)
	if e.Actor == "" {
		e.Actor = audit.ControllerActor
	}
	if e.StableRevision == "" {
		e.StableRevision = st.StableRevision
	}
	if e.CanaryRevision == "" {
		e.CanaryRevision = st.CanaryRevision
	}
	e.Step, e.CanaryWeight = st.CurrentStep, st.CanaryWeight
	*r.journal = append(*r.journal, e)
}

// flushAudit writes the pass's entries before its status is saved, so no
// decision takes effect without a record. If the write fails and an entry is
// blocking (a promotion, step advance, or rollout start), the pass fails and
// is retried. Other entries, including rollbacks, are dropped with a warning:
// an audit outage must never keep a bad canary serving traffic.
func (r *ModelRolloutReconciler) flushAudit(ctx context.Context, mr *mgv1.ModelRollout) error {
	if r.journal == nil || len(*r.journal) == 0 {
		return nil
	}
	entries := *r.journal
	*r.journal = nil
	err := r.auditLog().Write(ctx, entries)
	if err == nil {
		return nil
	}
	for _, e := range entries {
		if e.Blocking {
			return fmt.Errorf("recording %s in audit log: %w", e.Action, err)
		}
	}
	log.FromContext(ctx).Error(err, "audit write failed; continuing because no entry is blocking", "entries", len(entries))
	r.Recorder.Eventf(mr, corev1.EventTypeWarning, "AuditWriteFailed", "%d audit entries not recorded: %v", len(entries), err)
	return nil
}

func (r *ModelRolloutReconciler) setReadiness(mr *mgv1.ModelRollout) {
	st := mr.Status
	switch st.Phase {
	case mgv1.PhaseHealthy:
		setCondition(mr, mgv1.ConditionReady, metav1.ConditionTrue, "Stable", st.Message)
		meta.RemoveStatusCondition(&mr.Status.Conditions, mgv1.ConditionReconciling)
		meta.RemoveStatusCondition(&mr.Status.Conditions, mgv1.ConditionStalled)
	case mgv1.PhaseProgressing:
		setCondition(mr, mgv1.ConditionReady, metav1.ConditionFalse, "RolloutInProgress", st.Message)
		setCondition(mr, mgv1.ConditionReconciling, metav1.ConditionTrue, "RolloutInProgress", st.Message)
	case mgv1.PhasePaused:
		setCondition(mr, mgv1.ConditionReady, metav1.ConditionFalse, "Paused", st.Message)
		setCondition(mr, mgv1.ConditionReconciling, metav1.ConditionTrue, "Paused", st.Message)
	case mgv1.PhaseDegraded:
		// The stable version still serves, but the requested template did not ship.
		setCondition(mr, mgv1.ConditionReady, metav1.ConditionFalse, "RolledBack", st.Message)
		setCondition(mr, mgv1.ConditionStalled, metav1.ConditionTrue, "RolledBack", st.Message)
		meta.RemoveStatusCondition(&mr.Status.Conditions, mgv1.ConditionReconciling)
	}
}

func setCondition(mr *mgv1.ModelRollout, t string, s metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&mr.Status.Conditions, metav1.Condition{
		Type: t, Status: s, Reason: reason, Message: truncate(msg, 1024), ObservedGeneration: mr.Generation,
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func verdictStatus(v analysis.Verdict, stable, canary string, now time.Time) *mgv1.VerdictStatus {
	out := &mgv1.VerdictStatus{
		Decision: v.Decision, Reason: truncate(v.Reason, 1024), Time: metav1.NewTime(now), Look: int32(v.Look),
		StableSamples: v.StableSamples, CanarySamples: v.CanarySamples, PairedSamples: v.PairedSamples,
	}
	for _, c := range v.Checks {
		cs := mgv1.CheckStatus{Name: c.Name, Status: c.Status, Message: truncate(c.Message, 512)}
		if c.Status == analysis.Pass || c.Status == analysis.Fail {
			cs.Value = strconv.FormatFloat(c.Value, 'g', 4, 64)
			cs.Threshold = strconv.FormatFloat(c.Threshold, 'g', 4, 64)
		}
		out.Checks = append(out.Checks, cs)
	}
	for i, d := range v.Disagreements {
		if i == 5 {
			break
		}
		out.Disagreements = append(out.Disagreements, mgv1.DisagreementSample{
			RequestID: d.RequestID, Stable: predictionSample(d.ByRev[stable]), Canary: predictionSample(d.ByRev[canary]),
		})
	}
	return out
}

func predictionSample(p analysis.Prediction) mgv1.PredictionSample {
	var s mgv1.PredictionSample
	if p.Label != nil {
		s.Label = *p.Label
	}
	if p.Score != nil {
		s.Score = strconv.FormatFloat(*p.Score, 'g', 4, 64)
	}
	return s
}

// SetupWithManager registers the controller.
func (r *ModelRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mgv1.ModelRollout{}).
		Owns(&appsv1.Deployment{}).
		Watches(&mgv1.ModelGatePolicy{}, handler.EnqueueRequestsFromMapFunc(r.rolloutsInNamespace)).
		Named("modelrollout").
		Complete(r)
}

// rolloutsInNamespace re-reconciles every rollout when its namespace policy changes.
func (r *ModelRolloutReconciler) rolloutsInNamespace(ctx context.Context, obj client.Object) []reconcile.Request {
	var list mgv1.ModelRolloutList
	if err := r.List(ctx, &list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	reqs := make([]reconcile.Request, 0, len(list.Items))
	for _, mr := range list.Items {
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&mr)})
	}
	return reqs
}

// applyPolicy raises the rollout's analysis and strategy to the namespace
// ModelGatePolicy. If the policy cannot be read the reconcile fails and is
// retried: a rollout never proceeds on thresholds that might be too loose.
func (r *ModelRolloutReconciler) applyPolicy(ctx context.Context, mr *mgv1.ModelRollout) (*mgv1.ModelGatePolicySpec, error) {
	pol := &mgv1.ModelGatePolicy{}
	err := r.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mgv1.PolicyName}, pol)
	if apierrors.IsNotFound(err) {
		meta.RemoveStatusCondition(&mr.Status.Conditions, ConditionPolicyApplied)
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading namespace policy: %w", err)
	}
	res := policy.Apply(&pol.Spec, &mr.Spec)
	reason, msg := "Compliant", "rollout meets the namespace policy"
	if res.Changed() {
		reason = "Tightened"
		msg = "raised to namespace policy: " + strings.Join(append(res.Tightened, res.Added...), "; ")
	}
	setCondition(mr, ConditionPolicyApplied, metav1.ConditionTrue, reason, msg)
	return &pol.Spec, nil
}
