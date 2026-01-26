from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score, classification_report
import joblib
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    feat_path = Path(cfg["features_path"])
    model_path = Path(cfg["model_path"])
    ensure_dir(model_path.parent)

    df = pd.read_parquet(feat_path)

    y = df["churn_30d"].astype(int)
    X = df.drop(columns=["churn_30d"])

    cat = ["plan","country"]
    num = [c for c in X.columns if c not in cat and c != "user_id"]

    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
        ("num", "passthrough", num),
    ])

    clf = LogisticRegression(max_iter=500, n_jobs=1)

    pipe = Pipeline([("pre", pre), ("clf", clf)])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.22, random_state=42, stratify=y)

    pipe.fit(X_train, y_train)
    proba = pipe.predict_proba(X_test)[:,1]
    pred = (proba >= 0.5).astype(int)

    auc = float(roc_auc_score(y_test, proba))
    acc = float(accuracy_score(y_test, pred))

    artifact = {
        "trained_at": utcnow_iso(),
        "auc": auc,
        "accuracy": acc,
        "n_train": int(len(X_train)),
        "n_test": int(len(X_test)),
        "feature_columns": list(X.columns),
        "categoricals": cat,
        "numerics": num
    }

    joblib.dump({"model": pipe, "metadata": artifact}, model_path)

    print(json.dumps(artifact, indent=2))

if __name__ == "__main__":
    main()
