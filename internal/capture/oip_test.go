package capture

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/modelgate/modelgate/internal/analysis"
)

// outTensor encodes an InferOutputTensor. contents is an encoded
// InferTensorContents, or nil when raw_output_contents carries the data.
func outTensor(name, datatype string, shape []int64, contents []byte) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, name)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, datatype)
	var packed []byte
	for _, d := range shape {
		packed = protowire.AppendVarint(packed, uint64(d))
	}
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, packed)
	if contents != nil {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, contents)
	}
	return b
}

func fp32Contents(vals ...float32) []byte {
	var packed []byte
	for _, v := range vals {
		packed = protowire.AppendFixed32(packed, math.Float32bits(v))
	}
	b := protowire.AppendTag(nil, 6, protowire.BytesType)
	return protowire.AppendBytes(b, packed)
}

func bytesContents(vals ...string) []byte {
	var b []byte
	for _, v := range vals {
		b = protowire.AppendTag(b, 8, protowire.BytesType)
		b = protowire.AppendString(b, v)
	}
	return b
}

// inferResponse encodes a ModelInferResponse.
func inferResponse(outputs [][]byte, raws [][]byte) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, "fraud")
	for _, o := range outputs {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, o)
	}
	for _, r := range raws {
		b = protowire.AppendTag(b, 6, protowire.BytesType)
		b = protowire.AppendBytes(b, r)
	}
	return b
}

func grpcFrame(msg []byte) []byte {
	b := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(b[1:], uint32(len(msg)))
	return append(b, msg...)
}

func rawFP32(vals ...float32) []byte {
	b := make([]byte, 0, 4*len(vals))
	for _, v := range vals {
		b = binary.LittleEndian.AppendUint32(b, math.Float32bits(v))
	}
	return b
}

func extractWith(t *testing.T, protocol string, label, score *TensorSelector, body []byte) analysis.Event {
	t.Helper()
	p, err := NewProxy(Config{Upstream: &url.URL{Scheme: "http", Host: "x"}, Protocol: protocol, LabelTensor: label, ScoreTensor: score}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var e analysis.Event
	p.extract(body, &e)
	return e
}

func TestKServeV2RESTExtraction(t *testing.T) {
	body := []byte(`{"model_name":"fraud","outputs":[
		{"name":"label","datatype":"BYTES","shape":[2],"data":["benign","malignant"]},
		{"name":"probs","datatype":"FP32","shape":[2,3],"data":[[0.1,0.7,0.2],[0.9,0.05,0.05]]}]}`)
	e := extractWith(t, ProtocolKServeV2, &TensorSelector{Name: "probs", Reduce: "argmax"}, &TensorSelector{Name: "probs", Reduce: "max"}, body)
	if e.Invalid != "" || e.Label == nil || *e.Label != "1" || e.Score == nil || math.Abs(*e.Score-0.7) > 1e-9 {
		t.Fatalf("argmax/max of the first row: %+v label=%v score=%v", e, deref(e.Label), e.Score)
	}
	e = extractWith(t, ProtocolKServeV2, &TensorSelector{Name: "label"}, nil, body)
	if e.Label == nil || *e.Label != "benign" {
		t.Errorf("first of BYTES tensor: %+v", e)
	}
	e = extractWith(t, ProtocolKServeV2, &TensorSelector{Name: "missing"}, nil, body)
	if e.Invalid != "no output tensor missing" {
		t.Errorf("missing tensor: %q", e.Invalid)
	}
	e = extractWith(t, ProtocolKServeV2, nil, &TensorSelector{Name: "p"}, []byte(`{"outputs":[{"name":"p","datatype":"FP32","shape":[1],"data":[null]}]}`))
	if e.Invalid != "score is NaN or infinite" {
		t.Errorf("null score: %q", e.Invalid)
	}
}

func TestKServeV2GRPCDecoding(t *testing.T) {
	half := func(f float32) []byte { // 0.75 and 0.25 are exact in FP16
		h := map[float32]uint16{0.75: 0x3a00, 0.25: 0x3400}[f]
		return binary.LittleEndian.AppendUint16(nil, h)
	}
	cases := map[string]struct {
		outputs [][]byte
		raws    [][]byte
		label   *TensorSelector
		score   *TensorSelector
		want    string
		score0  float64
	}{
		"typed contents": {
			outputs: [][]byte{outTensor("probs", "FP32", []int64{1, 2}, fp32Contents(0.25, 0.75)), outTensor("cls", "BYTES", []int64{1}, bytesContents("benign"))},
			label:   &TensorSelector{Name: "cls"}, score: &TensorSelector{Name: "probs", Reduce: "max"}, want: "benign", score0: 0.75,
		},
		"raw FP32 contents": {
			outputs: [][]byte{outTensor("probs", "FP32", []int64{2, 2}, nil)},
			raws:    [][]byte{rawFP32(0.8, 0.2, 0.1, 0.9)},
			label:   &TensorSelector{Name: "probs", Reduce: "argmax"}, score: &TensorSelector{Name: "probs"}, want: "0", score0: 0.8000000119,
		},
		"raw FP16 contents": {
			outputs: [][]byte{outTensor("probs", "FP16", []int64{1, 2}, nil)},
			raws:    [][]byte{append(half(0.25), half(0.75)...)},
			label:   &TensorSelector{Name: "probs", Reduce: "argmax"}, score: &TensorSelector{Name: "probs", Reduce: "max"}, want: "1", score0: 0.75,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := extractWith(t, ProtocolKServeV2GRPC, tc.label, tc.score, grpcFrame(inferResponse(tc.outputs, tc.raws)))
			if e.Invalid != "" || deref(e.Label) != tc.want || e.Score == nil || math.Abs(*e.Score-tc.score0) > 1e-6 {
				t.Errorf("got invalid=%q label=%q score=%v", e.Invalid, deref(e.Label), e.Score)
			}
		})
	}
	compressed := grpcFrame(inferResponse(nil, nil))
	compressed[0] = 1
	if e := extractWith(t, ProtocolKServeV2GRPC, &TensorSelector{Name: "x"}, nil, compressed); e.Invalid != "compressed gRPC response" {
		t.Errorf("compressed: %q", e.Invalid)
	}
	if e := extractWith(t, ProtocolKServeV2GRPC, &TensorSelector{Name: "x"}, nil, grpcFrame([]byte{0xff})); !strings.HasPrefix(e.Invalid, "bad ModelInferResponse") {
		t.Errorf("garbage: %q", e.Invalid)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// rawCodec passes protobuf bytes through, so the test needs no generated code.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) { return *(v.(*[]byte)), nil }
func (rawCodec) Unmarshal(b []byte, v any) error {
	*(v.(*[]byte)) = append([]byte(nil), b...)
	return nil
}
func (rawCodec) Name() string   { return "proto" }
func (rawCodec) String() string { return "raw" }

var _ encoding.Codec = rawCodec{}

type events struct {
	mu sync.Mutex
	ev []analysis.Event
}

func (s *events) Publish(e analysis.Event) {
	s.mu.Lock()
	s.ev = append(s.ev, e)
	s.mu.Unlock()
}

func (s *events) wait(t *testing.T, n int) []analysis.Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		got := append([]analysis.Event(nil), s.ev...)
		s.mu.Unlock()
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestGRPCThroughSidecar sends real gRPC calls through the proxy to a real
// gRPC server, as Triton would serve them.
func TestGRPCThroughSidecar(t *testing.T) {
	handler := func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		var req []byte
		if err := dec(&req); err != nil {
			return nil, err
		}
		if string(req) == "bad" {
			return nil, status.Error(codes.InvalidArgument, "bad input")
		}
		if string(req) == "boom" {
			return nil, status.Error(codes.Internal, "model crashed")
		}
		resp := inferResponse([][]byte{outTensor("probs", "FP32", []int64{1, 2}, nil)}, [][]byte{rawFP32(0.3, 0.7)})
		return &resp, nil
	}
	gs := grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	gs.RegisterService(&grpc.ServiceDesc{
		ServiceName: "inference.GRPCInferenceService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "ModelInfer", Handler: handler},
			{MethodName: "ServerLive", Handler: handler},
		},
	}, struct{}{})
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	sink := &events{}
	proxy, err := NewProxy(Config{
		Upstream: &url.URL{Scheme: "http", Host: lis.Addr().String()}, Rollout: "ml/fraud", Revision: "r1", SampleRate: 1,
		Protocol: ProtocolKServeV2GRPC, LabelTensor: &TensorSelector{Name: "probs", Reduce: "argmax"},
		ScoreTensor: &TensorSelector{Name: "probs", Reduce: "max"},
	}, sink)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewUnstartedServer(proxy)
	front.Config.Protocols = new(http.Protocols)
	front.Config.Protocols.SetHTTP1(true)
	front.Config.Protocols.SetUnencryptedHTTP2(true)
	front.Start()
	t.Cleanup(front.Close)

	conn, err := grpc.NewClient(strings.TrimPrefix(front.URL, "http://"),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	call := func(method, body string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, out := []byte(body), []byte(nil)
		return conn.Invoke(ctx, "/inference.GRPCInferenceService/"+method, &req, &out)
	}
	if err := call("ModelInfer", "ok"); err != nil {
		t.Fatalf("ModelInfer through the sidecar: %v", err)
	}
	if err := call("ServerLive", "ok"); err != nil {
		t.Fatalf("ServerLive through the sidecar: %v", err)
	}
	if status.Code(call("ModelInfer", "bad")) != codes.InvalidArgument || status.Code(call("ModelInfer", "boom")) != codes.Internal {
		t.Fatal("gRPC errors not passed through")
	}

	got := sink.wait(t, 3)
	if len(got) != 3 {
		t.Fatalf("%d events, want 3 (ServerLive is not an inference call): %+v", len(got), got)
	}
	ok, bad, boom := got[0], got[1], got[2]
	if ok.Status != 200 || deref(ok.Label) != "1" || ok.Score == nil || math.Abs(*ok.Score-0.7) > 1e-6 || ok.Invalid != "" {
		t.Errorf("ok call: %+v label=%q", ok, deref(ok.Label))
	}
	if bad.Status != 400 || bad.IsError() || bad.Label != nil {
		t.Errorf("INVALID_ARGUMENT: status %d", bad.Status)
	}
	if boom.Status != 500 || !boom.IsError() {
		t.Errorf("INTERNAL: status %d", boom.Status)
	}
}

func TestOnlyInferenceCallsAreAnalyzed(t *testing.T) {
	p, err := NewProxy(Config{Upstream: &url.URL{Scheme: "http", Host: "x"}, Protocol: ProtocolKServeV2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for target, want := range map[string]bool{
		"POST /v2/models/fraud/infer":            true,
		"POST /v2/models/fraud/versions/3/infer": true,
		"GET /v2/health/ready":                   false,
		"GET /v2/models/fraud":                   false,
		"GET /v2/models/fraud/infer":             false,
	} {
		method, path, _ := strings.Cut(target, " ")
		if got := p.analyzed(httptest.NewRequest(method, path, nil)); got != want {
			t.Errorf("%s: analyzed %v, want %v", target, got, want)
		}
	}
	if _, err := NewProxy(Config{Upstream: &url.URL{Host: "x"}, Protocol: ProtocolKServeV2, LabelPath: "$.a"}, nil); err == nil {
		t.Error("label path accepted with kserveV2")
	}
	if _, err := NewProxy(Config{Upstream: &url.URL{Host: "x"}, LabelTensor: &TensorSelector{Name: "a"}}, nil); err == nil {
		t.Error("label tensor accepted with protocol json")
	}
}
