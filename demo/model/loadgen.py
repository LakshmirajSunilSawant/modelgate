"""Steady load generator for the demo model.

Replays rows of the breast cancer dataset (with small noise) against a URL,
so the canary and stable versions see the same traffic mix.

  python loadgen.py --url http://localhost:8080/predict --rps 50 --duration 1800
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
    args = ap.parse_args()

    rows = load_breast_cancer().data.tolist()
    stats = {"ok": 0, "err": 0}
    lock = threading.Lock()
    deadline = time.time() + args.duration
    interval = args.workers / args.rps

    def worker():
        while time.time() < deadline:
            start = time.time()
            row = [v * random.uniform(0.97, 1.03) for v in random.choice(rows)]
            req = urllib.request.Request(
                args.url,
                data=json.dumps({"features": row}).encode(),
                headers={"Content-Type": "application/json", "X-Request-Id": uuid.uuid4().hex},
            )
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    resp.read()
                key = "ok"
            except Exception:
                key = "err"
            with lock:
                stats[key] += 1
            time.sleep(max(0, interval - (time.time() - start)))

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.workers)]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        time.sleep(10)
        with lock:
            print(f"ok={stats['ok']} err={stats['err']}", flush=True)


if __name__ == "__main__":
    main()
