"""Steady load generator for the demo model.

Replays rows of the breast cancer dataset (with small noise) against a URL,
so the canary and stable versions see the same traffic mix.

  python loadgen.py --url http://localhost:8080/predict --rps 50 --duration 1800
  python loadgen.py --protocol v2 --url http://localhost:8080/v2/models/demo/infer
  python loadgen.py --protocol grpc --url localhost:8080

With --outcomes-url it also plays the part of a feedback pipeline: the true
diagnosis of each row is posted to the analyzer's /v1/outcomes endpoint
after --label-delay seconds, for a --label-rate share of requests. This is
what a labelAccuracy check consumes.
"""

import argparse
import json
import random
import threading
import time
import urllib.request
import uuid

from sklearn.datasets import load_breast_cancer


def sender(protocol, url):
    """Returns send(features, request_id), which raises on failure."""
    if protocol == "grpc":
        import grpc

        import oip

        channel = grpc.insecure_channel(url)
        infer = channel.unary_unary(oip.INFER_METHOD, request_serializer=oip.ModelInferRequest.SerializeToString,
                                    response_deserializer=oip.ModelInferResponse.FromString)

        def send_grpc(row, rid):
            infer(oip.infer_request(row, rid), timeout=5, metadata=(("x-request-id", rid),))
        return send_grpc

    def send_http(row, rid):
        if protocol == "v2":
            body = {"id": rid, "inputs": [{"name": "features", "datatype": "FP64", "shape": [1, len(row)], "data": row}]}
        else:
            body = {"features": row}
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json", "X-Request-Id": rid})
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    return send_http


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--rps", type=float, default=50)
    ap.add_argument("--duration", type=float, default=600, help="seconds")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--outcomes-url", help="analyzer /v1/outcomes URL; enables delayed labels")
    ap.add_argument("--rollout", default="demo/demo-model", help="namespace/name for outcomes")
    ap.add_argument("--label-delay", type=float, default=30, help="seconds before a label arrives")
    ap.add_argument("--label-rate", type=float, default=0.5, help="share of requests that get a label")
    ap.add_argument("--protocol", choices=["json", "v2", "grpc"], default="json",
                    help="json: POST {features}; v2: KServe v2 REST to --url; grpc: KServe v2 gRPC to --url host:port")
    ap.add_argument("--check", action="store_true", help="send one request and exit 0 if it succeeds")
    args = ap.parse_args()
    send = sender(args.protocol, args.url)

    data = load_breast_cancer()
    rows = data.data.tolist()
    truth = [data.target_names[t] for t in data.target]
    stats = {"ok": 0, "err": 0, "labels": 0}
    labels = []  # (due time, outcome)
    labels_lock = threading.Lock()
    lock = threading.Lock()
    deadline = time.time() + args.duration
    interval = args.workers / args.rps
    if args.check:
        try:
            send(rows[0], uuid.uuid4().hex)
        except Exception as e:
            print(f"check failed: {e}")
            raise SystemExit(1)
        return

    def worker():
        while time.time() < deadline:
            start = time.time()
            i = random.randrange(len(rows))
            row = [v * random.uniform(0.97, 1.03) for v in rows[i]]
            rid = uuid.uuid4().hex
            try:
                send(row, rid)
                key = "ok"
                if args.outcomes_url and random.random() < args.label_rate:
                    with labels_lock:
                        labels.append((time.time() + args.label_delay,
                                       {"rollout": args.rollout, "requestId": rid, "label": truth[i]}))
            except Exception as e:
                key = "err"
                with lock:
                    if stats["err"] < 3:
                        print(f"request failed: {str(e)[:300]}", flush=True)
            with lock:
                stats[key] += 1
            time.sleep(max(0, interval - (time.time() - start)))

    def feedback():
        while time.time() < deadline + args.label_delay:
            time.sleep(2)
            now = time.time()
            with labels_lock:
                due = [o for t, o in labels if t <= now]
                labels[:] = [(t, o) for t, o in labels if t > now]
            if not due:
                continue
            req = urllib.request.Request(args.outcomes_url, data=json.dumps(due).encode(),
                                         headers={"Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                with lock:
                    stats["labels"] += len(due)
            except Exception as e:
                print(f"outcomes post failed: {e}", flush=True)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    if args.outcomes_url:
        threading.Thread(target=feedback, daemon=True).start()
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        time.sleep(10)
        with lock:
            print(f"ok={stats['ok']} err={stats['err']} labels={stats['labels']}", flush=True)


if __name__ == "__main__":
    main()
