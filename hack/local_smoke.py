"""Local end-to-end smoke test, no Kubernetes needed.

Starts the analyzer, two demo model servers (stable=good, canary=<variant>),
and a capture sidecar in front of each. Sends traffic to the stable sidecar
and mirrors every request (same X-Request-Id) to the canary sidecar, which
is what Gateway API RequestMirror does in shadow mode. Then asks the
analyzer for a verdict.

  go build -o bin/ ./cmd/...
  python hack/local_smoke.py --variant swapped
"""

import argparse
import json
import os
import random
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXE = ".exe" if os.name == "nt" else ""


def wait_http(url, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        try:
            urllib.request.urlopen(url, timeout=1).read()
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError(f"{url} did not come up")


def post(url, body, headers=None):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.status, r.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", default="swapped")
    ap.add_argument("--requests", type=int, default=1500)
    ap.add_argument("--latency-ms", default="20", help="base model latency; must dominate server overhead for latency checks")
    args = ap.parse_args()

    procs = []

    def start(cmd, env=None, cwd=ROOT):
        p = subprocess.Popen(cmd, env={**os.environ, **(env or {})}, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p)
        return p

    try:
        start([os.path.join(ROOT, "bin", "analyzer" + EXE), "--listen=127.0.0.1:18090"])
        for port, variant in [(18080, "good"), (18081, args.variant)]:
            start([sys.executable, "-m", "uvicorn", "app:app", "--host", "127.0.0.1", "--port", str(port)],
                  env={"MODEL_VARIANT": variant, "BASE_LATENCY_MS": args.latency_ms}, cwd=os.path.join(ROOT, "demo", "model"))
        for port, admin, upstream, rev in [(18099, 18098, 18080, "stable1"), (18199, 18198, 18081, "canary1")]:
            start([os.path.join(ROOT, "bin", "sidecar" + EXE)], env={
                "MODELGATE_LISTEN": f"127.0.0.1:{port}", "MODELGATE_ADMIN_LISTEN": f"127.0.0.1:{admin}",
                "MODELGATE_UPSTREAM": f"http://127.0.0.1:{upstream}", "MODELGATE_ROLLOUT": "demo/demo-model",
                "MODELGATE_REVISION": rev, "MODELGATE_LABEL_PATH": "$.prediction.label",
                "MODELGATE_SCORE_PATH": "$.prediction.confidence", "MODELGATE_SCORE_MIN": "0", "MODELGATE_SCORE_MAX": "1",
                "MODELGATE_ANALYZER_URL": "http://127.0.0.1:18090/v1/events",
            })
        for u in ["http://127.0.0.1:18090/healthz", "http://127.0.0.1:18080/healthz", "http://127.0.0.1:18081/healthz",
                  "http://127.0.0.1:18098/healthz", "http://127.0.0.1:18198/healthz"]:
            wait_http(u)

        since = datetime.now(timezone.utc) - timedelta(seconds=30)
        from sklearn.datasets import load_breast_cancer
        rows = load_breast_cancer().data.tolist()
        lat_direct, lat_proxied = [], []
        for i in range(args.requests):
            row = [v * random.uniform(0.97, 1.03) for v in random.choice(rows)]
            rid = uuid.uuid4().hex
            t0 = time.perf_counter()
            post("http://127.0.0.1:18099/predict", {"features": row}, {"X-Request-Id": rid})
            lat_proxied.append(time.perf_counter() - t0)
            post("http://127.0.0.1:18199/predict", {"features": row}, {"X-Request-Id": rid})
            if i % 10 == 0:
                t0 = time.perf_counter()
                post("http://127.0.0.1:18080/predict", {"features": row})
                lat_direct.append(time.perf_counter() - t0)
        time.sleep(2.5)  # let sidecar batches flush

        req = {
            "rollout": "demo/demo-model", "stable": "stable1", "canary": "canary1",
            "since": since.isoformat(), "phase": "shadow", "look": 1, "minSamples": 1000,
            "checks": [
                {"name": "agreement", "type": "agreementRate", "min": 0.97, "phase": "shadow"},
                {"name": "pred-shift", "type": "psi", "max": 0.10},
                {"name": "score-shape", "type": "ks", "max": 0.10},
                {"name": "valid", "type": "validity", "max": 0},
                {"name": "latency", "type": "p99Ratio", "max": 1.20},
                {"name": "errors", "type": "errorRate", "max": 0.01},
            ],
        }
        _, body = post("http://127.0.0.1:18090/v1/verdict", req)
        v = json.loads(body)
        print(f"variant={args.variant} decision={v['decision']} hard={v['hardFailure']}")
        print(f"  reason: {v['reason']}")
        print(f"  samples: stable={v['stableSamples']} canary={v['canarySamples']} paired={v['pairedSamples']}")
        for c in v["checks"]:
            print(f"  {c['status']:>12}  {c['message']}")
        lat_direct.sort()
        lat_proxied.sort()
        med = lambda xs: 1000 * xs[len(xs) // 2]
        print(f"  median latency direct={med(lat_direct):.2f}ms via sidecar={med(lat_proxied):.2f}ms")
    finally:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()
