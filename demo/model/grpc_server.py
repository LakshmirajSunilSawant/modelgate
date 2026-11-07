"""gRPC (KServe v2 / Open Inference Protocol) server for the demo model.

Serves the same MODEL_VARIANT as app.py over gRPC, the way Triton does:
ModelInfer returns a BYTES "label" tensor and an FP32 "confidence" tensor
as raw output contents.

  python grpc_server.py            # listens on GRPC_PORT, default 8080
"""

import os
import random
import time
from concurrent import futures

import grpc

import oip
from app import BASE_LATENCY_S, VARIANT, predict_row


def model_infer(request, context):
    delay = BASE_LATENCY_S * (2 if VARIANT == "slow" else 1)
    time.sleep(delay * random.uniform(0.7, 1.3))
    try:
        features = oip.request_features(request)
        pred = predict_row(features)
    except Exception as e:  # bad input is the client's fault
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"bad input: {e}")
    return oip.infer_response(pred["label"], pred["confidence"])


def server_live(request, context):
    return oip.ModelInferResponse()


def main():
    port = int(os.environ.get("GRPC_PORT", "8080"))
    handler = grpc.method_handlers_generic_handler(oip.SERVICE, {
        "ModelInfer": grpc.unary_unary_rpc_method_handler(
            model_infer, request_deserializer=oip.ModelInferRequest.FromString,
            response_serializer=oip.ModelInferResponse.SerializeToString),
        # Health check stand-in, so tests can show non-inference calls pass through.
        "ServerLive": grpc.unary_unary_rpc_method_handler(
            server_live, request_deserializer=oip.ModelInferRequest.FromString,
            response_serializer=oip.ModelInferResponse.SerializeToString),
    })
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
    server.add_generic_rpc_handlers((handler,))
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"variant={VARIANT} gRPC on :{port}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    main()
