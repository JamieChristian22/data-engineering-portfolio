
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .features import run as build_features, Paths
from .logutil import get_logger

log = get_logger("p1.train_model")


@dataclass(frozen=True)
class Config:
    features_csv: Path
    model_path: Path
    metrics_path: Path
    seed: int


def train(cfg: Config) -> None:
    df = pd.read_csv(cfg.features_csv)

    target = "label_purchase"
    y = df[target]
    X = df.drop(columns=[target, "start_time", "end_time"])

    cat = ["device", "referrer", "country"]
    num = [c for c in X.columns if c not in cat and c != "session_id" and c != "user_id"]

    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
            ("num", Pipeline([("scaler", StandardScaler())]), num),
        ],
        remainder="drop",
    )

    model = LogisticRegression(max_iter=1500, class_weight="balanced", n_jobs=None)

    pipe = Pipeline([("pre", pre), ("model", model)])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=cfg.seed, stratify=y)
    pipe.fit(X_train, y_train)

    proba = pipe.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, proba)
    ap = average_precision_score(y_test, proba)

    cfg.model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipe, cfg.model_path)

    cfg.metrics_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.metrics_path.write_text(f"roc_auc={auc:.4f}\navg_precision={ap:.4f}\n")

    log.info("Saved model -> %s", cfg.model_path)
    log.info("Metrics: ROC AUC=%.4f | AvgPrecision=%.4f", auc, ap)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    raw = root / "data" / "raw" / "events.csv"
    features_csv = root / "data" / "processed" / "session_features.csv"
    model_path = root / "models" / "propensity_model.joblib"
    metrics_path = root / "reports" / "model_metrics.txt"

    if not raw.exists():
        raise FileNotFoundError(f"Missing raw events at {raw}. Run: python -m src.generate_data")

    if not features_csv.exists():
        build_features(Paths(raw, features_csv))

    train(Config(features_csv, model_path, metrics_path, args.seed))


if __name__ == "__main__":
    main()
