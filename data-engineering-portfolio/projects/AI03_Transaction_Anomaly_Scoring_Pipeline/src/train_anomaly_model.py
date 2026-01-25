from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import IsolationForest
import joblib
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    tx_path = Path(cfg["transactions_path"])
    model_path = Path(cfg["model_path"])
    ensure_dir(model_path.parent)

    df = pd.read_csv(tx_path)

    y = df["is_chargeback"].astype(int)
    X = df.drop(columns=["is_chargeback"])

    cat = ["merchant_id","country","payment_method","device"]
    num = ["amount_usd"]

    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
        ("num", "passthrough", num)
    ])

    model = IsolationForest(
        n_estimators=300,
        contamination=0.01,
        random_state=42
    )

    pipe = Pipeline([("pre", pre), ("model", model)])
    pipe.fit(X)

    artifact = {
        "trained_at": utcnow_iso(),
        "rows": int(len(df)),
        "contamination": 0.01,
        "columns": list(X.columns)
    }
    joblib.dump({"model": pipe, "metadata": artifact}, model_path)
    print(json.dumps(artifact, indent=2))

if __name__ == "__main__":
    main()
