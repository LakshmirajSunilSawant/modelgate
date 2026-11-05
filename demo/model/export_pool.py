"""Exports each demo variant's predictions on one shared pool of requests.

The benchmark (hack/benchmark) replays requests from this pool, so stable
and canary predictions for a request come from the real scikit-learn
models without running a server per run.

  python demo/model/export_pool.py --out bench/pool.json
"""

import argparse
import importlib
import json
import math
import os
import random
import sys

VARIANTS = ["good", "identical", "swapped", "scaling", "stale", "accuracy-drop", "nan", "slow"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--size", type=int, default=5000)
    args = ap.parse_args()

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from sklearn.datasets import load_breast_cancer

    rng = random.Random(0)
    rows = load_breast_cancer().data.tolist()
    # Same perturbation as loadgen.py: live traffic resembles, but is not, the training data.
    pool = [[v * rng.uniform(0.97, 1.03) for v in rng.choice(rows)] for _ in range(args.size)]

    out = {"size": args.size, "variants": {}}
    for v in VARIANTS:
        os.environ["MODEL_VARIANT"] = v
        app = importlib.reload(importlib.import_module("app")) if "app" in sys.modules else importlib.import_module("app")
        labels, scores = [], []
        for x in pool:
            p = app.predict_row(x)
            labels.append(p["label"])
            scores.append(None if math.isnan(p["confidence"]) else p["confidence"])
        out["variants"][v] = {"labels": labels, "scores": scores}
        print(f"{v}: {sum(s is None for s in scores)} NaN, benign share {labels.count('benign') / len(labels):.3f}")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(out, f)


if __name__ == "__main__":
    main()
