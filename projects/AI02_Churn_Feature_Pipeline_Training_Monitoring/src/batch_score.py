from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import joblib
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    feat_path = Path(cfg["features_path"])
    model_path = Path(cfg["model_path"])
    pred_path = Path(cfg["predictions_path"])
    ensure_dir(pred_path.parent)

    df = pd.read_parquet(feat_path)
    artifact = joblib.load(model_path)
    model = artifact["model"]

    X = df.drop(columns=["churn_30d"])
    proba = model.predict_proba(X)[:,1]
    out = pd.DataFrame({
        "scored_at": utcnow_iso(),
        "user_id": X["user_id"].astype(int),
        "churn_risk": proba
    }).sort_values("churn_risk", ascending=False)

    out.to_csv(pred_path, index=False)
    print(f"Wrote predictions: {pred_path.resolve()} (top risk={out['churn_risk'].iloc[0]:.3f})")

if __name__ == "__main__":
    main()
