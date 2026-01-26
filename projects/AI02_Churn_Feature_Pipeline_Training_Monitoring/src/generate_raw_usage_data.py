from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from shared.utils import ensure_dir

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    raw_path = Path(cfg["raw_path"])
    ensure_dir(raw_path.parent)

    rng = np.random.default_rng(202)
    n_users = 5000
    user_ids = np.arange(1, n_users+1)

    start = datetime(2025, 7, 1)
    days = 120

    # user base attributes
    plan = rng.choice(["basic","pro","enterprise"], p=[0.72,0.25,0.03], size=n_users)
    tenure_days = rng.integers(1, 900, size=n_users)
    country = rng.choice(["US","CA","GB","AU","DE","IN"], p=[0.55,0.12,0.10,0.08,0.08,0.07], size=n_users)

    rows = []
    for d in range(days):
        dt = start + timedelta(days=d)
        active_mask = rng.random(n_users) < 0.60  # daily activity probability
        sessions = rng.poisson(lam=2.2, size=n_users) * active_mask
        tickets = rng.poisson(lam=0.08, size=n_users) * active_mask
        # usage minutes depends on plan
        base = np.where(plan=="basic", 18, np.where(plan=="pro", 32, 55))
        usage = (rng.normal(base, 9, size=n_users).clip(0) * active_mask).round(1)
        # payments
        mrr = np.where(plan=="basic", 19.0, np.where(plan=="pro", 49.0, 199.0))
        rows.append(pd.DataFrame({
            "event_date": dt.date().isoformat(),
            "user_id": user_ids,
            "plan": plan,
            "country": country,
            "tenure_days": tenure_days + d,
            "sessions": sessions,
            "support_tickets": tickets,
            "usage_minutes": usage,
            "mrr_usd": mrr
        }))

    df = pd.concat(rows, ignore_index=True)

    # Create churn label: probability increases with tickets, low usage, short tenure, and basic plan
    # label is assigned at user-level based on last 30d behavior
    last30 = df[df["event_date"] >= (start + timedelta(days=days-30)).date().isoformat()]
    agg = last30.groupby("user_id").agg(
        sessions_30=("sessions","sum"),
        tickets_30=("support_tickets","sum"),
        usage_30=("usage_minutes","sum"),
        tenure_end=("tenure_days","max"),
        plan=("plan","last"),
        mrr=("mrr_usd","last")
    )
    rng2 = np.random.default_rng(303)
    risk = (
        0.10
        + 0.04 * (agg["tickets_30"] > 2).astype(float)
        + 0.06 * (agg["usage_30"] < 250).astype(float)
        + 0.05 * (agg["sessions_30"] < 20).astype(float)
        + 0.03 * (agg["tenure_end"] < 90).astype(float)
        + 0.03 * (agg["plan"] == "basic").astype(float)
        - 0.02 * (agg["plan"] == "enterprise").astype(float)
    ).clip(0.02, 0.55)

    churn = (rng2.random(len(risk)) < risk).astype(int)
    churn_df = pd.DataFrame({"user_id": agg.index.values, "churn_30d": churn})
    df = df.merge(churn_df, on="user_id", how="left")

    df.to_csv(raw_path, index=False)
    print(f"Wrote raw usage data: {raw_path.resolve()} ({len(df):,} rows)")

if __name__ == "__main__":
    main()
