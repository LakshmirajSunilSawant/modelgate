"""Open Inference Protocol (KServe v2) messages for the demo, built at runtime.

Only the fields the demo uses are declared, with the field numbers of
KServe's grpc_predict_v2.proto, so these messages are wire-compatible with
Triton and other servers. Building them from a descriptor avoids needing
protoc or generated code.
"""

import struct

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

SERVICE = "inference.GRPCInferenceService"
INFER_METHOD = f"/{SERVICE}/ModelInfer"

_F = descriptor_pb2.FieldDescriptorProto


def _build():
    fdp = descriptor_pb2.FileDescriptorProto(name="modelgate_demo_oip.proto", package="inference", syntax="proto3")

    def message(name, fields):
        m = fdp.message_type.add(name=name)
        for fname, number, ftype, label, type_name in fields:
            f = m.field.add(name=fname, number=number, type=ftype, label=label)
            if type_name:
                f.type_name = type_name

    one, many = _F.LABEL_OPTIONAL, _F.LABEL_REPEATED
    message("InferTensorContents", [
        ("fp32_contents", 6, _F.TYPE_FLOAT, many, None),
        ("fp64_contents", 7, _F.TYPE_DOUBLE, many, None),
        ("bytes_contents", 8, _F.TYPE_BYTES, many, None),
    ])
    tensor = [
        ("name", 1, _F.TYPE_STRING, one, None),
        ("datatype", 2, _F.TYPE_STRING, one, None),
        ("shape", 3, _F.TYPE_INT64, many, None),
        ("contents", 5, _F.TYPE_MESSAGE, one, ".inference.InferTensorContents"),
    ]
    message("InferInputTensor", tensor)
    message("InferOutputTensor", tensor)
    message("ModelInferRequest", [
        ("model_name", 1, _F.TYPE_STRING, one, None),
        ("id", 3, _F.TYPE_STRING, one, None),
        ("inputs", 5, _F.TYPE_MESSAGE, many, ".inference.InferInputTensor"),
        ("raw_input_contents", 7, _F.TYPE_BYTES, many, None),
    ])
    message("ModelInferResponse", [
        ("model_name", 1, _F.TYPE_STRING, one, None),
        ("id", 3, _F.TYPE_STRING, one, None),
        ("outputs", 5, _F.TYPE_MESSAGE, many, ".inference.InferOutputTensor"),
        ("raw_output_contents", 6, _F.TYPE_BYTES, many, None),
    ])
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    get = lambda n: message_factory.GetMessageClass(pool.FindMessageTypeByName("inference." + n))
    return get("ModelInferRequest"), get("ModelInferResponse")


ModelInferRequest, ModelInferResponse = _build()


def infer_request(features, request_id=""):
    req = ModelInferRequest(model_name="demo", id=request_id)
    t = req.inputs.add(name="features", datatype="FP64", shape=[1, len(features)])
    t.contents.fp64_contents.extend(features)
    return req


def request_features(req):
    """Reads the features from typed contents or raw_input_contents."""
    t = req.inputs[0]
    if req.raw_input_contents:
        raw = req.raw_input_contents[0]
        return list(struct.unpack(f"<{len(raw) // 8}d", raw))
    return list(t.contents.fp64_contents)


def infer_response(label, confidence):
    """Like Triton, which sends output data as raw little-endian bytes."""
    resp = ModelInferResponse(model_name="demo")
    resp.outputs.add(name="label", datatype="BYTES", shape=[1, 1])
    b = label.encode()
    resp.raw_output_contents.append(struct.pack("<I", len(b)) + b)
    resp.outputs.add(name="confidence", datatype="FP32", shape=[1, 1])
    resp.raw_output_contents.append(struct.pack("<f", confidence))
    return resp
