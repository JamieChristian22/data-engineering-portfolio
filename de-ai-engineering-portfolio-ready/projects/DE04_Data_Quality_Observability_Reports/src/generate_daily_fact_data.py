from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    data_dir = ensure_dir(Path(cfg["data_dir"]))
    rng = np.random.default_rng(123)

    # Create 14 days of daily "facts" with a controlled anomaly on day 11 (refund spike)
    start = datetime(2025, 12, 15, tzinfo=timezone.utc)
    days = [start + timedelta(days=i) for i in range(14)]

    rows = []
    for d in days:
        n = int(rng.integers(4500, 6500))
        refund_rate = 0.03
        if d.date() == (start + timedelta(days=10)).date():
            refund_rate = 0.12  # anomaly
        for i in range(n):
            amt = float(np.round(max(0, rng.normal(55, 22)), 2))
            is_refund = int(rng.random() < refund_rate)
            rows.append({
                "event_date": d.date().isoformat(),
                "order_id": f"ORD-{d.strftime('%Y%m%d')}-{i:05d}",
                "customer_id": int(rng.integers(1, 1200)),
                "amount_usd": amt,
                "is_refund": is_refund,
                "loaded_at": (d + timedelta(hours=6)).isoformat()
            })

    df = pd.DataFrame(rows)
    out = data_dir / "daily_orders_fact.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {len(df):,} rows to {out.resolve()}")

if __name__ == "__main__":
    main()
