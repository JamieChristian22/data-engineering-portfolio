from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd
import joblib
from scipy.stats import ks_2samp
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    feat_path = Path(cfg["features_path"])
    model_path = Path(cfg["model_path"])
    drift_path = Path(cfg["drift_report_path"])
    ensure_dir(drift_path.parent)

    df = pd.read_parquet(feat_path)

    # Simulate "current" batch by adding mild behavior shift
    rng = np.random.default_rng(77)
    current = df.copy()
    current["usage_30"] = (current["usage_30"] * rng.normal(0.96, 0.05, size=len(current))).clip(0)
    current["tickets_30"] = (current["tickets_30"] + rng.poisson(0.1, size=len(current))).clip(0)

    baseline = df.sample(frac=0.5, random_state=1)
    current = current.sample(frac=0.5, random_state=2)

    artifact = joblib.load(model_path)["metadata"]
    numeric_cols = artifact["numerics"]

    drift = []
    for col in numeric_cols:
        a = baseline[col].astype(float).values
        b = current[col].astype(float).values
        stat, p = ks_2samp(a, b)
        drift.append({"feature": col, "ks_stat": float(stat), "p_value": float(p)})

    # Rule: flag if KS > 0.12 and p < 0.01
    flagged = [d for d in drift if d["ks_stat"] > 0.12 and d["p_value"] < 0.01]

    report = {
        "run_at": utcnow_iso(),
        "baseline_n": int(len(baseline)),
        "current_n": int(len(current)),
        "rule": {"ks_stat_gt": 0.12, "p_value_lt": 0.01},
        "flagged_features": flagged,
        "all_features": sorted(drift, key=lambda x: x["ks_stat"], reverse=True)
    }

    drift_path.write_text(json.dumps(report, indent=2))
    print(json.dumps({"flagged": [f["feature"] for f in flagged], "top_ks": report["all_features"][:3]}, indent=2))

if __name__ == "__main__":
    main()
