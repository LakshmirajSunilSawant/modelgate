package controller

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
)

const (
	labelRollout  = "modelgate.dev/rollout"
	labelRevision = "modelgate.dev/revision"
	sidecarName   = "modelgate-capture"
	capturePort   = 8099
	adminPort     = 8098
	servicePort   = 80
)

// hashJSON is a short stable hash of any JSON-serializable value.
func hashJSON(v any) string {
	b, _ := json.Marshal(v)
	h := fnv.New64a()
	h.Write(b)
	return fmt.Sprintf("%016x", h.Sum64())[:10]
}

// Revision hashes the pod template. Equal templates map to equal revisions,
// so reverting the template returns to the stable revision without a rollout.
func Revision(tpl *corev1.PodTemplateSpec) string {
	b, _ := json.Marshal(tpl)
	h := fnv.New64a()
	h.Write(b)
	return fmt.Sprintf("%010x", h.Sum64())[:10]
}

func deploymentName(mr *mgv1.ModelRollout, rev string) string { return mr.Name + "-" + rev }
func stableServiceName(mr *mgv1.ModelRollout) string          { return mr.Name + "-stable" }
func canaryServiceName(mr *mgv1.ModelRollout) string          { return mr.Name + "-canary" }
func rolloutKey(mr *mgv1.ModelRollout) string                 { return mr.Namespace + "/" + mr.Name }

func ownerRef(mr *mgv1.ModelRollout) metav1.OwnerReference {
	return *metav1.NewControllerRef(mr, mgv1.GroupVersion.WithKind("ModelRollout"))
}

func modelPort(mr *mgv1.ModelRollout) int32 {
	if mr.Spec.ModelPort > 0 {
		return mr.Spec.ModelPort
	}
	return 8080
}

func replicas(mr *mgv1.ModelRollout) int32 {
	if mr.Spec.Replicas != nil {
		return *mr.Spec.Replicas
	}
	return 1
}

// SidecarConfig is how the controller wires the capture sidecar.
type SidecarConfig struct {
	Image       string
	AnalyzerURL string // used by the sidecar when NATSURL is empty
	NATSURL     string
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }

// buildDeployment renders the Deployment for one revision, with the capture
// sidecar injected in front of the model server.
func buildDeployment(mr *mgv1.ModelRollout, rev string, sc SidecarConfig) *appsv1.Deployment {
	labels := map[string]string{labelRollout: mr.Name, labelRevision: rev}
	tpl := mr.Spec.Template.DeepCopy()
	if tpl.Labels == nil {
		tpl.Labels = map[string]string{}
	}
	for k, v := range labels {
		tpl.Labels[k] = v
	}
	tpl.Spec.Containers = append(withoutContainer(tpl.Spec.Containers, sidecarName), buildSidecar(mr, rev, sc))

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:            deploymentName(mr, rev),
			Namespace:       mr.Namespace,
			Labels:          labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(mr)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr(replicas(mr)),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: *tpl,
		},
	}
}

// buildSidecar renders the capture container. It depends on spec.output and
// spec.capture, not the pod template, so it can change without a new revision.
func buildSidecar(mr *mgv1.ModelRollout, rev string, sc SidecarConfig) corev1.Container {
	sampleRate := mr.Spec.Capture.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}
	env := []corev1.EnvVar{
		{Name: "MODELGATE_LISTEN", Value: fmt.Sprintf(":%d", capturePort)},
		{Name: "MODELGATE_ADMIN_LISTEN", Value: fmt.Sprintf(":%d", adminPort)},
		{Name: "MODELGATE_UPSTREAM", Value: fmt.Sprintf("http://127.0.0.1:%d", modelPort(mr))},
		{Name: "MODELGATE_ROLLOUT", Value: rolloutKey(mr)},
		{Name: "MODELGATE_REVISION", Value: rev},
		{Name: "MODELGATE_LABEL_PATH", Value: mr.Spec.Output.LabelPath},
		{Name: "MODELGATE_SCORE_PATH", Value: mr.Spec.Output.ScorePath},
		{Name: "MODELGATE_SAMPLE_RATE", Value: formatFloat(sampleRate)},
		{Name: "MODELGATE_ANALYZER_URL", Value: sc.AnalyzerURL},
		{Name: "MODELGATE_NATS_URL", Value: sc.NATSURL},
	}
	if r := mr.Spec.Output.ScoreRange; r != nil {
		if r.Min != nil {
			env = append(env, corev1.EnvVar{Name: "MODELGATE_SCORE_MIN", Value: formatFloat(*r.Min)})
		}
		if r.Max != nil {
			env = append(env, corev1.EnvVar{Name: "MODELGATE_SCORE_MAX", Value: formatFloat(*r.Max)})
		}
	}
	// Set only for the kserveV2 protocols, so JSON rollouts keep their
	// sidecar hash and are not restarted by an upgrade.
	if out := mr.Spec.Output; out.Protocol != "" && out.Protocol != mgv1.ProtocolJSON {
		env = append(env, corev1.EnvVar{Name: "MODELGATE_PROTOCOL", Value: string(out.Protocol)})
		for _, t := range []struct {
			name string
			ref  *mgv1.TensorRef
		}{{"MODELGATE_LABEL_TENSOR", out.LabelTensor}, {"MODELGATE_SCORE_TENSOR", out.ScoreTensor}} {
			if t.ref != nil {
				b, _ := json.Marshal(t.ref)
				env = append(env, corev1.EnvVar{Name: t.name, Value: string(b)})
			}
		}
	}
	if pc := mr.Spec.Capture.Payloads; payloadCaptureAllowed(mr) {
		rate := pc.SampleRate
		if rate == 0 {
			rate = 0.01
		}
		redact, _ := json.Marshal(pc.Redact)
		env = append(env,
			corev1.EnvVar{Name: "MODELGATE_PAYLOAD_UNTIL", Value: pc.Until.UTC().Format(time.RFC3339)},
			corev1.EnvVar{Name: "MODELGATE_PAYLOAD_SAMPLE_RATE", Value: formatFloat(rate)},
			corev1.EnvVar{Name: "MODELGATE_PAYLOAD_REDACT", Value: string(redact)},
		)
		if pc.MaxBytes > 0 {
			env = append(env, corev1.EnvVar{Name: "MODELGATE_PAYLOAD_MAX_BYTES", Value: fmt.Sprint(pc.MaxBytes)})
		}
	}
	sidecar := corev1.Container{
		Name:  sidecarName,
		Image: sc.Image,
		Env:   env,
		Ports: []corev1.ContainerPort{
			{Name: "capture", ContainerPort: capturePort},
			{Name: "capture-admin", ContainerPort: adminPort},
		},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(adminPort)},
		}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    mustQuantity("50m"),
				corev1.ResourceMemory: mustQuantity("32Mi"),
			},
			Limits: corev1.ResourceList{corev1.ResourceMemory: mustQuantity("128Mi")},
		},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr(false),
			ReadOnlyRootFilesystem:   ptr(true),
			RunAsNonRoot:             ptr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	return sidecar
}

func withoutContainer(cs []corev1.Container, name string) []corev1.Container {
	out := make([]corev1.Container, 0, len(cs))
	for _, c := range cs {
		if c.Name != name {
			out = append(out, c)
		}
	}
	return out
}

// buildService renders a Service that selects one revision's pods and
// targets the capture sidecar, so every request passes through it.
func buildService(mr *mgv1.ModelRollout, name, rev string) *corev1.Service {
	port := corev1.ServicePort{Name: "http", Port: servicePort, TargetPort: intstr.FromString("capture")}
	if mr.Spec.Output.Protocol == mgv1.ProtocolKServeV2GRPC {
		// Tells the gateway to speak HTTP/2 without TLS to the sidecar.
		port.Name, port.AppProtocol = "grpc", ptr("kubernetes.io/h2c")
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       mr.Namespace,
			Labels:          map[string]string{labelRollout: mr.Name},
			OwnerReferences: []metav1.OwnerReference{ownerRef(mr)},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{labelRollout: mr.Name, labelRevision: rev},
			Ports:    []corev1.ServicePort{port},
		},
	}
}

func deploymentAvailable(d *appsv1.Deployment) bool {
	if d.Generation > d.Status.ObservedGeneration {
		return false
	}
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	return d.Status.AvailableReplicas >= want && d.Status.UpdatedReplicas >= want
}

func mustQuantity(s string) resource.Quantity { return resource.MustParse(s) }

func ptr[T any](v T) *T { return &v }
