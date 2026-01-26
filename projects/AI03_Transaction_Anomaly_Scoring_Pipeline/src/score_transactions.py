from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import numpy as np
import joblib
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    tx_path = Path(cfg["transactions_path"])
    model_path = Path(cfg["model_path"])
    scores_path = Path(cfg["scores_path"])
    ensure_dir(scores_path.parent)

    df = pd.read_csv(tx_path)
    artifact = joblib.load(model_path)
    model = artifact["model"]

    X = df.drop(columns=["is_chargeback"])
    # IsolationForest: decision_function higher means more normal; we invert to anomaly score [0,1]
    normality = model.decision_function(X)
    anomaly_score = (1 - (normality - normality.min()) / (normality.max() - normality.min() + 1e-9))

    out = df.copy()
    out["scored_at"] = utcnow_iso()
    out["anomaly_score"] = anomaly_score.round(4)
    out["alert"] = (out["anomaly_score"] >= float(cfg["alert_threshold"])).astype(int)

    out.sort_values("anomaly_score", ascending=False).to_csv(scores_path, index=False)
    print(f"Wrote scores: {scores_path.resolve()} alerts={(out['alert']==1).sum()}")

if __name__ == "__main__":
    main()
