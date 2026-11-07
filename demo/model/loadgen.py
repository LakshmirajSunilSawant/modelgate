"""Steady load generator for the demo model.

Replays rows of the breast cancer dataset (with small noise) against a URL,
so the canary and stable versions see the same traffic mix.

  python loadgen.py --url http://localhost:8080/predict --rps 50 --duration 1800

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
    args = ap.parse_args()

    data = load_breast_cancer()
    rows = data.data.tolist()
    truth = [data.target_names[t] for t in data.target]
    stats = {"ok": 0, "err": 0, "labels": 0}
    labels = []  # (due time, outcome)
    labels_lock = threading.Lock()
    lock = threading.Lock()
    deadline = time.time() + args.duration
    interval = args.workers / args.rps

    def worker():
        while time.time() < deadline:
            start = time.time()
            i = random.randrange(len(rows))
            row = [v * random.uniform(0.97, 1.03) for v in rows[i]]
            rid = uuid.uuid4().hex
            req = urllib.request.Request(
                args.url,
                data=json.dumps({"features": row}).encode(),
                headers={"Content-Type": "application/json", "X-Request-Id": rid},
            )
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                key = "ok"
                if args.outcomes_url and random.random() < args.label_rate:
                    with labels_lock:
                        labels.append((time.time() + args.label_delay,
                                       {"rollout": args.rollout, "requestId": rid, "label": truth[i]}))
            except Exception:
                key = "err"
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
