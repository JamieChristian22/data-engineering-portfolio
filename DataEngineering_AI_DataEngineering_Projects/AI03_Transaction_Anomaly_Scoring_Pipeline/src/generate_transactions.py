from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from shared.utils import ensure_dir

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    path = Path(cfg["transactions_path"])
    ensure_dir(path.parent)

    rng = np.random.default_rng(404)
    n = 25000

    merchants = [f"m_{i:04d}" for i in range(1, 250)]
    countries = ["US","CA","GB","AU","DE","FR","BR","IN","NG","MX"]
    methods = ["card","paypal","apple_pay","google_pay"]
    devices = ["ios","android","web"]

    # Normal behavior
    amount = rng.lognormal(mean=3.4, sigma=0.55, size=n).round(2)  # mostly small/medium
    txn_time = pd.date_range("2025-12-01", periods=n, freq="min")
    df = pd.DataFrame({
        "txn_id": [f"TXN-{i:06d}" for i in range(1, n+1)],
        "txn_ts": txn_time.astype(str),
        "merchant_id": rng.choice(merchants, size=n),
        "country": rng.choice(countries, p=[0.52,0.08,0.07,0.05,0.06,0.05,0.04,0.06,0.03,0.04], size=n),
        "payment_method": rng.choice(methods, p=[0.72,0.12,0.10,0.06], size=n),
        "device": rng.choice(devices, p=[0.34,0.36,0.30], size=n),
        "amount_usd": amount,
        "is_chargeback": 0
    })

    # Inject anomalies: very high amounts, rare countries, and chargebacks
    idx = rng.choice(df.index, size=250, replace=False)
    df.loc[idx, "amount_usd"] = (df.loc[idx, "amount_usd"] * rng.uniform(10, 40, size=len(idx))).round(2)
    df.loc[idx[:80], "country"] = rng.choice(["NG","BR","MX"], size=80)
    df.loc[idx[:120], "is_chargeback"] = 1

    df.to_csv(path, index=False)
    print(f"Wrote transactions: {path.resolve()} ({len(df):,} rows, injected anomalies={len(idx)})")

if __name__ == "__main__":
    main()
