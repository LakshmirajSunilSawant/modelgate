package capture

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// This file reads predictions from Open Inference Protocol (KServe v2)
// responses, over REST or gRPC, as served by Triton, KServe, MLServer and
// others. Predictions are chosen by output tensor name, so no per-model
// parsing code is needed.

// Protocols the sidecar understands.
const (
	ProtocolJSON         = "json"
	ProtocolKServeV2     = "kserveV2"
	ProtocolKServeV2GRPC = "kserveV2gRPC"
)

// grpcInferMethod is the only gRPC call whose responses are analyzed.
const grpcInferMethod = "/inference.GRPCInferenceService/ModelInfer"

// TensorSelector picks one value from a named output tensor.
type TensorSelector struct {
	Name   string `json:"name"`
	Reduce string `json:"reduce,omitempty"` // first (default), argmax, or max
}

func (s *TensorSelector) validate() error {
	if s.Name == "" {
		return errors.New("tensor name is required")
	}
	switch s.Reduce {
	case "", "first", "argmax", "max":
		return nil
	}
	return fmt.Errorf("tensor %s: unknown reduce %q", s.Name, s.Reduce)
}

// tensor is one output tensor. Values are decoded only for the first row.
type tensor struct {
	name     string
	datatype string
	shape    []int64
	// Exactly one source holds the values.
	jsonData []any  // REST: the data array, possibly nested
	contents []byte // gRPC: an InferTensorContents message
	raw      []byte // gRPC: the matching raw_output_contents entry
}

// rowLen is the number of elements in the first row: everything after the
// batch dimension, or the whole tensor if it has at most one dimension.
func (t *tensor) rowLen() (int, error) {
	if len(t.shape) <= 1 {
		if len(t.shape) == 1 {
			return int(min(t.shape[0], 1<<20)), nil
		}
		return 1, nil
	}
	n := int64(1)
	for _, d := range t.shape[1:] {
		if d < 0 {
			return 0, fmt.Errorf("tensor %s has a negative dimension", t.name)
		}
		n *= d
		if n > 1<<20 {
			return 0, fmt.Errorf("tensor %s rows are too large", t.name)
		}
	}
	return int(n), nil
}

// row returns the first row's values as float64, string, or bool.
func (t *tensor) row() ([]any, error) {
	n, err := t.rowLen()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, fmt.Errorf("tensor %s is empty", t.name)
	}
	var vals []any
	switch {
	case t.jsonData != nil:
		vals = flattenJSON(t.jsonData, n, nil)
	case t.raw != nil:
		vals, err = decodeRaw(t.datatype, t.raw, n)
	default:
		vals, err = decodeContents(t.contents, n)
	}
	if err != nil {
		return nil, fmt.Errorf("tensor %s: %w", t.name, err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("tensor %s has no data", t.name)
	}
	return vals, nil
}

// reduce turns the tensor's first row into one value.
func (s *TensorSelector) reduce(t *tensor) (any, error) {
	vals, err := t.row()
	if err != nil {
		return nil, err
	}
	switch s.Reduce {
	case "", "first":
		return vals[0], nil
	}
	best, bestAt := math.Inf(-1), -1
	for i, v := range vals {
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("tensor %s: %s needs numbers", t.name, s.Reduce)
		}
		if math.IsNaN(f) {
			if s.Reduce == "max" {
				return f, nil // reported as a NaN score
			}
			return nil, fmt.Errorf("tensor %s holds NaN", t.name)
		}
		if bestAt < 0 || f > best {
			best, bestAt = f, i
		}
	}
	if s.Reduce == "argmax" {
		return float64(bestAt), nil
	}
	return best, nil
}

func findTensor(ts []tensor, name string) *tensor {
	for i := range ts {
		if ts[i].name == name {
			return &ts[i]
		}
	}
	return nil
}

// parseV2JSON reads the outputs of a REST inference response.
func parseV2JSON(body []byte) ([]tensor, error) {
	var resp struct {
		Outputs []struct {
			Name     string  `json:"name"`
			Datatype string  `json:"datatype"`
			Shape    []int64 `json:"shape"`
			Data     any     `json:"data"`
		} `json:"outputs"`
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&resp); err != nil {
		return nil, errors.New("unparseable response body")
	}
	out := make([]tensor, 0, len(resp.Outputs))
	for _, o := range resp.Outputs {
		data, ok := o.Data.([]any)
		if !ok {
			data = []any{o.Data}
		}
		out = append(out, tensor{name: o.Name, datatype: o.Datatype, shape: o.Shape, jsonData: data})
	}
	return out, nil
}

// flattenJSON reads up to n scalars from a possibly nested array.
func flattenJSON(data []any, n int, out []any) []any {
	for _, v := range data {
		if len(out) >= n {
			break
		}
		switch t := v.(type) {
		case []any:
			out = flattenJSON(t, n, out)
		case json.Number:
			f, err := t.Float64()
			if err != nil {
				out = append(out, t.String())
			} else {
				out = append(out, f)
			}
		case string, bool:
			out = append(out, t)
		case nil:
			out = append(out, math.NaN()) // what some servers write for NaN
		}
	}
	return out
}

// parseV2GRPC reads the outputs of a unary ModelInfer response: one gRPC
// length-prefixed message holding a ModelInferResponse.
func parseV2GRPC(body []byte) ([]tensor, error) {
	if len(body) < 5 {
		return nil, errors.New("no gRPC message in response")
	}
	if body[0] != 0 {
		return nil, errors.New("compressed gRPC response")
	}
	size := binary.BigEndian.Uint32(body[1:5])
	if uint64(len(body)-5) < uint64(size) {
		return nil, errors.New("truncated gRPC message")
	}
	msg := body[5 : 5+size]

	var outs []tensor
	var raws [][]byte
	err := eachField(msg, func(num protowire.Number, typ protowire.Type, v []byte, _ uint64) error {
		switch {
		case num == 5 && typ == protowire.BytesType: // outputs
			t, err := parseOutputTensor(v)
			if err != nil {
				return err
			}
			outs = append(outs, t)
		case num == 6 && typ == protowire.BytesType: // raw_output_contents
			raws = append(raws, v)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("bad ModelInferResponse: %w", err)
	}
	// raw_output_contents, when used, has one entry per output, in order.
	if len(raws) > 0 {
		if len(raws) != len(outs) {
			return nil, errors.New("bad ModelInferResponse: raw_output_contents does not match outputs")
		}
		for i := range outs {
			outs[i].raw = raws[i]
		}
	}
	return outs, nil
}

func parseOutputTensor(b []byte) (tensor, error) {
	var t tensor
	err := eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, x uint64) error {
		switch num {
		case 1:
			t.name = string(v)
		case 2:
			t.datatype = string(v)
		case 3: // shape, packed or not
			if typ == protowire.VarintType {
				t.shape = append(t.shape, int64(x))
				return nil
			}
			return eachPacked(v, protowire.VarintType, func(x uint64) bool {
				t.shape = append(t.shape, int64(x))
				return true
			})
		case 5:
			t.contents = v
		}
		return nil
	})
	if t.contents == nil {
		t.contents = []byte{}
	}
	return t, err
}

// decodeContents reads up to n values from an InferTensorContents message.
// Only one of its repeated fields is set, matching the tensor's datatype.
func decodeContents(b []byte, n int) ([]any, error) {
	var out []any
	add := func(v any) bool {
		out = append(out, v)
		return len(out) < n
	}
	err := eachField(b, func(num protowire.Number, typ protowire.Type, v []byte, x uint64) error {
		if len(out) >= n {
			return nil
		}
		var conv func(uint64) any
		wire := protowire.VarintType
		switch num {
		case 1:
			conv = func(x uint64) any { return x != 0 }
		case 2:
			conv = func(x uint64) any { return float64(int32(x)) }
		case 3:
			conv = func(x uint64) any { return float64(int64(x)) }
		case 4, 5:
			conv = func(x uint64) any { return float64(x) }
		case 6:
			wire = protowire.Fixed32Type
			conv = func(x uint64) any { return float64(math.Float32frombits(uint32(x))) }
		case 7:
			wire = protowire.Fixed64Type
			conv = func(x uint64) any { return math.Float64frombits(x) }
		case 8:
			if typ == protowire.BytesType {
				add(string(v))
			}
			return nil
		default:
			return nil
		}
		if typ == protowire.BytesType {
			return eachPacked(v, wire, func(x uint64) bool { return add(conv(x)) })
		}
		add(conv(x))
		return nil
	})
	return out, err
}

// decodeRaw reads up to n values from little-endian raw tensor bytes.
func decodeRaw(datatype string, b []byte, n int) ([]any, error) {
	dt := strings.ToUpper(datatype)
	if dt == "BYTES" {
		var out []any
		for len(b) > 0 && len(out) < n {
			if len(b) < 4 {
				return nil, errors.New("truncated BYTES element")
			}
			l := binary.LittleEndian.Uint32(b)
			if uint64(len(b)-4) < uint64(l) {
				return nil, errors.New("truncated BYTES element")
			}
			out = append(out, string(b[4:4+l]))
			b = b[4+l:]
		}
		return out, nil
	}
	size := map[string]int{"BOOL": 1, "UINT8": 1, "INT8": 1, "UINT16": 2, "INT16": 2, "FP16": 2, "BF16": 2,
		"UINT32": 4, "INT32": 4, "FP32": 4, "UINT64": 8, "INT64": 8, "FP64": 8}[dt]
	if size == 0 {
		return nil, fmt.Errorf("unsupported datatype %q", datatype)
	}
	count := min(n, len(b)/size)
	out := make([]any, 0, count)
	for i := 0; i < count; i++ {
		e := b[i*size : (i+1)*size]
		var v any
		switch dt {
		case "BOOL":
			v = e[0] != 0
		case "UINT8":
			v = float64(e[0])
		case "INT8":
			v = float64(int8(e[0]))
		case "UINT16":
			v = float64(binary.LittleEndian.Uint16(e))
		case "INT16":
			v = float64(int16(binary.LittleEndian.Uint16(e)))
		case "FP16":
			v = float64(halfToFloat(binary.LittleEndian.Uint16(e)))
		case "BF16":
			v = float64(math.Float32frombits(uint32(binary.LittleEndian.Uint16(e)) << 16))
		case "UINT32":
			v = float64(binary.LittleEndian.Uint32(e))
		case "INT32":
			v = float64(int32(binary.LittleEndian.Uint32(e)))
		case "FP32":
			v = float64(math.Float32frombits(binary.LittleEndian.Uint32(e)))
		case "UINT64":
			v = float64(binary.LittleEndian.Uint64(e))
		case "INT64":
			v = float64(int64(binary.LittleEndian.Uint64(e)))
		case "FP64":
			v = math.Float64frombits(binary.LittleEndian.Uint64(e))
		}
		out = append(out, v)
	}
	return out, nil
}

// halfToFloat converts an IEEE 754 half-precision float.
func halfToFloat(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := uint32(h>>10) & 0x1f
	frac := uint32(h) & 0x3ff
	switch {
	case exp == 0x1f: // Inf or NaN
		return math.Float32frombits(sign | 0x7f800000 | frac<<13)
	case exp == 0 && frac == 0:
		return math.Float32frombits(sign)
	case exp == 0: // subnormal
		f := float32(frac) / (1 << 24)
		if sign != 0 {
			f = -f
		}
		return f
	}
	return math.Float32frombits(sign | (exp+112)<<23 | frac<<13)
}

// eachField calls fn for every field of a protobuf message. Bytes fields
// pass their contents in v; varint and fixed fields pass their value in x.
func eachField(b []byte, fn func(num protowire.Number, typ protowire.Type, v []byte, x uint64) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		var v []byte
		var x uint64
		switch typ {
		case protowire.VarintType:
			x, n = protowire.ConsumeVarint(b)
		case protowire.Fixed32Type:
			var u uint32
			u, n = protowire.ConsumeFixed32(b)
			x = uint64(u)
		case protowire.Fixed64Type:
			x, n = protowire.ConsumeFixed64(b)
		case protowire.BytesType:
			v, n = protowire.ConsumeBytes(b)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
		}
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if err := fn(num, typ, v, x); err != nil {
			return err
		}
	}
	return nil
}

// eachPacked walks a packed repeated scalar field until fn returns false.
func eachPacked(b []byte, wire protowire.Type, fn func(uint64) bool) error {
	for len(b) > 0 {
		var x uint64
		var n int
		switch wire {
		case protowire.VarintType:
			x, n = protowire.ConsumeVarint(b)
		case protowire.Fixed32Type:
			var u uint32
			u, n = protowire.ConsumeFixed32(b)
			x = uint64(u)
		case protowire.Fixed64Type:
			x, n = protowire.ConsumeFixed64(b)
		}
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if !fn(x) {
			return nil
		}
	}
	return nil
}

// grpcHTTPStatus maps a gRPC status code to the HTTP status the analyzer
// counts, following the usual gRPC-to-HTTP mapping: client mistakes are
// 4xx, server failures 5xx.
func grpcHTTPStatus(code string) int {
	switch code {
	case "0":
		return 200
	case "1":
		return 499 // cancelled by the client
	case "3", "9", "11":
		return 400
	case "4":
		return 504
	case "5":
		return 404
	case "6", "10":
		return 409
	case "7":
		return 403
	case "8":
		return 429
	case "12":
		return 501
	case "14":
		return 503
	case "16":
		return 401
	}
	return 500 // UNKNOWN, INTERNAL, DATA_LOSS, or anything unexpected
}
