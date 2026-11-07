package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	mgv1 "github.com/modelgate/modelgate/api/v1alpha1"
	"github.com/modelgate/modelgate/internal/capture"
)

// Reasons of the Replay condition.
const (
	replayReady       = "Ready"
	replayNoConfigMap = "ConfigMapNotFound"
	replayNoKey       = "KeyNotFound"
	replayBadRequests = "InvalidRequests"
	replayUnreadable  = "ConfigMapUnreadable"
)

const (
	defaultReplayKey  = "requests.jsonl"
	defaultReplayRate = 5
	replayTimeout     = 5 * time.Second
)

var replayRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "modelgate_replay_requests_total",
	Help: "Recorded requests replayed, by target (stable or canary) and outcome (ok, http_error, failed).",
}, []string{"namespace", "rollout", "target", "outcome"})

func init() { metrics.Registry.MustRegister(replayRequests) }

func replayKey(rp *mgv1.ReplaySpec) string {
	if rp.ConfigMapRef.Key != "" {
		return rp.ConfigMapRef.Key
	}
	return defaultReplayKey
}

// readReplayRequests loads and parses the recorded requests. The reason is
// one of the Replay condition reasons; it is empty on success.
func readReplayRequests(ctx context.Context, reader client.Reader, mr *mgv1.ModelRollout) (reqs []json.RawMessage, version, reason string, err error) {
	rp := mr.Spec.Strategy.Replay
	cm := &corev1.ConfigMap{}
	err = reader.Get(ctx, types.NamespacedName{Namespace: mr.Namespace, Name: rp.ConfigMapRef.Name}, cm)
	switch {
	case apierrors.IsNotFound(err):
		return nil, "", replayNoConfigMap, fmt.Errorf("ConfigMap %s not found", rp.ConfigMapRef.Name)
	case err != nil:
		return nil, "", replayUnreadable, err
	}
	key := replayKey(rp)
	data, ok := cm.Data[key]
	if !ok {
		b, inBinary := cm.BinaryData[key]
		if !inBinary {
			return nil, "", replayNoKey, fmt.Errorf("ConfigMap %s has no key %s", cm.Name, key)
		}
		data = string(b)
	}
	reqs, err = parseReplayRequests(data)
	if err != nil {
		return nil, "", replayBadRequests, fmt.Errorf("ConfigMap %s key %s: %w", cm.Name, key, err)
	}
	return reqs, cm.ResourceVersion, "", nil
}

// parseReplayRequests reads one JSON request body per line.
func parseReplayRequests(data string) ([]json.RawMessage, error) {
	var out []json.RawMessage
	sc := bufio.NewScanner(strings.NewReader(data))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			return nil, fmt.Errorf("line %d is not valid JSON", n)
		}
		out = append(out, json.RawMessage(bytes.Clone(line)))
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no requests")
	}
	return out, nil
}

// applyReplay records in the Replay condition whether the recorded requests
// can be read. The Replayer only sends requests while the condition is true.
func (r *ModelRolloutReconciler) applyReplay(ctx context.Context, mr *mgv1.ModelRollout) {
	rp := mr.Spec.Strategy.Replay
	if rp == nil {
		meta.RemoveStatusCondition(&mr.Status.Conditions, mgv1.ConditionReplay)
		return
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	reqs, _, reason, err := readReplayRequests(ctx, reader, mr)
	if err != nil {
		setCondition(mr, mgv1.ConditionReplay, metav1.ConditionFalse, reason, "replay is off: "+err.Error())
		return
	}
	setCondition(mr, mgv1.ConditionReplay, metav1.ConditionTrue, replayReady,
		fmt.Sprintf("%d recorded requests, replayed at %d/s to stable and canary during analysis", len(reqs), replayRate(rp)))
}

func replayRate(rp *mgv1.ReplaySpec) int32 {
	if rp.RatePerSecond > 0 {
		return rp.RatePerSecond
	}
	return defaultReplayRate
}

// replaying reports whether recorded requests should be sent now: a canary
// is under analysis, nothing holds the rollout, and the requests are readable.
func replaying(mr *mgv1.ModelRollout) bool {
	st := mr.Status
	return mr.Spec.Strategy.Replay != nil && st.CanaryRevision != "" && st.Phase == mgv1.PhaseProgressing &&
		!mr.Spec.Paused && meta.IsStatusConditionTrue(st.Conditions, mgv1.ConditionReplay)
}

// Replayer sends recorded requests to the stable and canary Services of
// every rollout that is replaying. It runs only on the leader. Each request
// goes to both versions with the same request ID, so the analyzer pairs the
// two responses like mirrored traffic.
type Replayer struct {
	// Client lists ModelRollouts, usually from the cache.
	Client client.Reader
	// Reader reads ConfigMaps without a cache.
	Reader client.Reader
	HTTP   *http.Client
	// BaseURL returns a Service's base URL. Tests point it at a test server.
	BaseURL func(namespace, service string) string
	// Tick is how often requests are sent; RatePerSecond is per second.
	Tick time.Duration

	mu    sync.Mutex
	runs  map[types.NamespacedName]*replayRun
	nonce string
}

type replayRun struct {
	canary   string
	version  string
	requests []json.RawMessage
	next     int
	seq      uint64
}

// NeedLeaderElection keeps replay to one controller replica.
func (r *Replayer) NeedLeaderElection() bool { return true }

// Start runs until the context ends.
func (r *Replayer) Start(ctx context.Context) error {
	tick := r.Tick
	if tick <= 0 {
		tick = time.Second
	}
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.RunOnce(ctx, tick)
		}
	}
}

// RunOnce sends one tick's worth of requests for every replaying rollout
// and waits for the responses.
func (r *Replayer) RunOnce(ctx context.Context, tick time.Duration) {
	var list mgv1.ModelRolloutList
	if err := r.Client.List(ctx, &list); err != nil {
		log.FromContext(ctx).Error(err, "replay: listing rollouts")
		return
	}
	r.mu.Lock()
	if r.runs == nil {
		r.runs = map[types.NamespacedName]*replayRun{}
		var b [4]byte
		_, _ = rand.Read(b[:])
		r.nonce = hex.EncodeToString(b[:])
	}
	var wg sync.WaitGroup
	seen := map[types.NamespacedName]bool{}
	for i := range list.Items {
		mr := &list.Items[i]
		if !replaying(mr) {
			continue
		}
		key := client.ObjectKeyFromObject(mr)
		seen[key] = true
		run := r.runs[key]
		if run == nil || run.canary != mr.Status.CanaryRevision {
			run = &replayRun{canary: mr.Status.CanaryRevision}
			r.runs[key] = run
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.replay(ctx, mr, run, tick)
		}()
	}
	for key := range r.runs {
		if !seen[key] {
			delete(r.runs, key)
		}
	}
	r.mu.Unlock()
	wg.Wait()
}

// replay sends this tick's requests for one rollout, spread over the tick.
func (r *Replayer) replay(ctx context.Context, mr *mgv1.ModelRollout, run *replayRun, tick time.Duration) {
	logger := log.FromContext(ctx).WithValues("rollout", rolloutKey(mr))
	if err := r.load(ctx, mr, run); err != nil {
		logger.Error(err, "replay: reading recorded requests")
		return
	}
	rp := mr.Spec.Strategy.Replay
	n := int(int64(replayRate(rp)) * int64(tick) / int64(time.Second))
	if n < 1 {
		n = 1
	}
	sampleRate := mr.Spec.Capture.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}
	path := rp.Path
	if path == "" {
		path = "/"
	}
	gap := tick / time.Duration(n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case <-time.After(gap):
			}
		}
		body := run.requests[run.next%len(run.requests)]
		run.next++
		id := r.requestID(run, sampleRate)
		for _, target := range []struct{ name, svc string }{
			{"stable", stableServiceName(mr)}, {"canary", canaryServiceName(mr)},
		} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				outcome := r.send(ctx, r.baseURL(mr.Namespace, target.svc)+path, id, body)
				replayRequests.WithLabelValues(mr.Namespace, mr.Name, target.name, outcome).Inc()
			}()
		}
	}
	wg.Wait()
}

// load reads the recorded requests when the ConfigMap has changed.
func (r *Replayer) load(ctx context.Context, mr *mgv1.ModelRollout, run *replayRun) error {
	reqs, version, _, err := readReplayRequests(ctx, r.Reader, mr)
	if err != nil {
		return err
	}
	if version != run.version || len(run.requests) == 0 {
		run.requests, run.version, run.next = reqs, version, 0
	}
	return nil
}

// requestID picks a new ID that the sidecars' sample rate captures, so
// every replayed request becomes a paired sample.
func (r *Replayer) requestID(run *replayRun, sampleRate float64) string {
	for tries := 0; ; tries++ {
		run.seq++
		id := fmt.Sprintf("replay-%s-%s-%d", r.nonce, run.canary, run.seq)
		if sampleRate <= 0 || tries >= 10000 || capture.Sampled(id, sampleRate) {
			return id
		}
	}
}

func (r *Replayer) baseURL(namespace, service string) string {
	if r.BaseURL != nil {
		return r.BaseURL(namespace, service)
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", service, namespace, servicePort)
}

func (r *Replayer) send(ctx context.Context, url, id string, body []byte) string {
	ctx, cancel := context.WithTimeout(ctx, replayTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "failed"
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", id)
	req.Header.Set("User-Agent", "modelgate-replay")
	c := r.HTTP
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return "failed"
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "http_error"
	}
	return "ok"
}
