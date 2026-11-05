"""Demo model server for ModelGate.

A scikit-learn classifier trained on the public Wisconsin breast cancer
dataset (bundled with scikit-learn, no download). The MODEL_VARIANT
environment variable selects a good version or one of the deliberately
broken versions from the PRD's bad-model suite:

  good           baseline
  identical      same as good, retrained with another seed (false-alarm runs)
  swapped        label mapping swapped
  scaling        feature-scaling bug in preprocessing
  stale          trained on a stale, shifted slice of the data
  accuracy-drop  about 5 points less accurate
  nan            returns NaN for some inputs
  slow           2x latency
"""

import asyncio
import json
import math
import os
import random

import numpy as np
from fastapi import FastAPI, Request
from fastapi.responses import Response
from sklearn.datasets import load_breast_cancer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

VARIANT = os.environ.get("MODEL_VARIANT", "good")
BASE_LATENCY_S = float(os.environ.get("BASE_LATENCY_MS", "20")) / 1000

data = load_breast_cancer()
X_train, X_test, y_train, y_test = train_test_split(
    data.data, data.target, test_size=0.3, random_state=0, stratify=data.target
)
CLASSES = list(data.target_names)  # ["malignant", "benign"]


def train(X, y, seed=0, **lr):
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=5000, random_state=seed, **lr))
    model.fit(X, y)
    return model


if VARIANT == "stale":
    # Stale data: an old sample skewed toward large tumors.
    keep = X_train[:, 0] > np.percentile(X_train[:, 0], 70)
    model = train(X_train[keep], y_train[keep])
elif VARIANT == "accuracy-drop":
    # Five features and stronger regularization: a weaker but plausible model.
    WEAK_COLS = [0, 1, 4, 8, 9]
    weak = train(X_train[:, WEAK_COLS], y_train, C=0.05)
    model = None
elif VARIANT == "identical":
    model = train(X_train, y_train, seed=1)
else:
    model = train(X_train, y_train)

app = FastAPI()


def predict_row(x):
    x = np.asarray(x, dtype=float).reshape(1, -1)
    if VARIANT == "scaling":
        x = x / 10.0  # preprocessing bug: unit conversion applied twice
    if VARIANT == "accuracy-drop":
        proba = weak.predict_proba(x[:, WEAK_COLS])[0]
    else:
        proba = model.predict_proba(x)[0]
    idx = int(np.argmax(proba))
    if VARIANT == "swapped":
        idx = 1 - idx
    confidence = float(proba[1])  # probability of "benign"
    if VARIANT == "nan" and x[0, 0] > np.percentile(X_train[:, 0], 90):
        confidence = math.nan
    return {"label": CLASSES[idx], "confidence": confidence}


@app.post("/predict")
async def predict(request: Request):
    body = await request.json()
    delay = BASE_LATENCY_S * (2 if VARIANT == "slow" else 1)
    await asyncio.sleep(delay * random.uniform(0.7, 1.3))  # never block the event loop
    pred = predict_row(body["features"])
    # json.dumps writes NaN as a bare token, as many real servers do.
    return Response(json.dumps({"prediction": pred, "variant": VARIANT}), media_type="application/json")


@app.get("/healthz")
def healthz():
    return {"ok": True}


if __name__ == "__main__":
    acc = np.mean([CLASSES.index(predict_row(x)["label"]) == y for x, y in zip(X_test, y_test)])
    print(f"variant={VARIANT} test accuracy={acc:.3f}")
