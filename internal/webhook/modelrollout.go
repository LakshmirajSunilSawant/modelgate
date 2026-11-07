// Package webhook validates ModelRollouts against their namespace's
// ModelGatePolicy, so teams learn at apply time that a threshold is too
// loose. The controller enforces the same policy at runtime regardless, so
// the webhook can fail open without weakening the gate.
package webhook

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/policy"
)

// Path is where the API server sends ModelRollout admission reviews.
const Path = "/validate-modelgate-dev-v1alpha1-modelrollout"

// RolloutValidator rejects rollouts looser than the namespace policy.
type RolloutValidator struct {
	Reader client.Reader
}

// Setup registers the webhook with the manager's webhook server.
func Setup(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&mgv1.ModelRollout{}).
		WithValidator(&RolloutValidator{Reader: mgr.GetAPIReader()}).
		WithValidatorCustomPath(Path).
		Complete()
}

func (v *RolloutValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return v.validate(ctx, obj)
}

func (v *RolloutValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	oldMR, ok1 := oldObj.(*mgv1.ModelRollout)
	newMR, ok2 := newObj.(*mgv1.ModelRollout)
	// Metadata-only updates (manual-action annotations, labels) must always
	// pass, even for rollouts created before the policy was tightened.
	if ok1 && ok2 && equality.Semantic.DeepEqual(oldMR.Spec, newMR.Spec) {
		return nil, nil
	}
	return v.validate(ctx, newObj)
}

func (v *RolloutValidator) ValidateDelete(context.Context, runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

func (v *RolloutValidator) validate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	mr, ok := obj.(*mgv1.ModelRollout)
	if !ok {
		return nil, fmt.Errorf("expected a ModelRollout, got %T", obj)
	}
	pol := &mgv1.ModelGatePolicy{}
	err := v.Reader.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: mgv1.PolicyName}, pol)
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		// Fail open: the controller still enforces the policy at runtime.
		return admission.Warnings{"could not read namespace ModelGatePolicy; the controller will still enforce it: " + err.Error()}, nil
	}
	res := policy.Apply(&pol.Spec, mr.Spec.DeepCopy())
	warnings := admission.Warnings(res.Added)
	if len(res.Tightened) > 0 {
		return warnings, fmt.Errorf("ModelRollout %s is looser than ModelGatePolicy %s/%s (teams may only tighten it): %s",
			mr.Name, mr.Namespace, mgv1.PolicyName, strings.Join(res.Tightened, "; "))
	}
	return warnings, nil
}
