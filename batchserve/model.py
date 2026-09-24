from functools import lru_cache

import numpy as np
from sklearn.linear_model import LogisticRegression

VERSIONS = {"linear-v1": 42, "linear-v2": 73}


@lru_cache(maxsize=2)
def load(version):
    rng = np.random.default_rng(VERSIONS[version])
    features = rng.normal(size=(500, 2))
    labels = (features[:, 0] + 0.7 * features[:, 1] > 0).astype(int)
    return LogisticRegression(random_state=VERSIONS[version]).fit(features, labels)


def predict(version, rows):
    features = np.array([[r["x1"], r["x2"]] for r in rows])
    probabilities = load(version).predict_proba(features)[:, 1]
    return [
        {"id": row["id"], "probability": round(float(p), 8), "prediction": int(p >= 0.5)}
        for row, p in zip(rows, probabilities)
    ]
