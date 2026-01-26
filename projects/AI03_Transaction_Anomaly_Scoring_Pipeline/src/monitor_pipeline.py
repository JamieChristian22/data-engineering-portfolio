from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import numpy as np
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    tx_path = Path(cfg["transactions_path"])
    scores_path = Path(cfg["scores_path"])
    report_path = Path(cfg["monitor_report_path"])
    ensure_dir(report_path.parent)

    df = pd.read_csv(tx_path)
    scored = pd.read_csv(scores_path) if scores_path.exists() else None

    volume = int(len(df))
    amount_mean = float(df["amount_usd"].mean())
    amount_p99 = float(df["amount_usd"].quantile(0.99))

    report = {
        "run_at": utcnow_iso(),
        "volume": volume,
        "amount_mean": amount_mean,
        "amount_p99": amount_p99,
        "alerts": int(scored["alert"].sum()) if scored is not None else 0,
        "alert_rate": float(scored["alert"].mean()) if scored is not None else 0.0,
        "rules": {
            "min_volume": 20000,
            "max_alert_rate": 0.02,
            "p99_amount_upper_guardrail": 2000.0
        },
        "violations": []
    }

    if volume < report["rules"]["min_volume"]:
        report["violations"].append("volume_below_min")
    if scored is not None and report["alert_rate"] > report["rules"]["max_alert_rate"]:
        report["violations"].append("alert_rate_too_high")
    if amount_p99 > report["rules"]["p99_amount_upper_guardrail"]:
        report["violations"].append("p99_amount_guardrail_breached")

    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps({"violations": report["violations"], "alerts": report["alerts"]}, indent=2))

if __name__ == "__main__":
    main()
