from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import duckdb
from shared.utils import ensure_dir

FEATURE_SQL = '\nWITH base AS (\n  SELECT\n    user_id,\n    plan,\n    country,\n    tenure_days,\n    sessions,\n    support_tickets,\n    usage_minutes,\n    mrr_usd,\n    churn_30d,\n    CAST(event_date AS DATE) AS event_date\n  FROM raw_usage\n),\nanchor AS (\n  SELECT max(event_date) AS max_date FROM base\n),\nlast30 AS (\n  SELECT b.*\n  FROM base b, anchor a\n  WHERE b.event_date >= a.max_date - INTERVAL 29 DAY\n),\nagg AS (\n  SELECT\n    user_id,\n    any_value(plan) AS plan,\n    any_value(country) AS country,\n    max(tenure_days) AS tenure_end,\n    any_value(mrr_usd) AS mrr_usd,\n    any_value(churn_30d) AS churn_30d,\n    sum(sessions) AS sessions_30,\n    sum(support_tickets) AS tickets_30,\n    sum(usage_minutes) AS usage_30,\n    sum(CASE WHEN sessions > 0 OR usage_minutes > 0 THEN 1 ELSE 0 END) AS days_active_30,\n    avg(sessions) AS sessions_avg_daily,\n    avg(usage_minutes) AS usage_avg_daily\n  FROM last30\n  GROUP BY 1\n)\nSELECT\n  user_id,\n  plan,\n  country,\n  tenure_end,\n  mrr_usd,\n  sessions_30,\n  tickets_30,\n  usage_30,\n  days_active_30,\n  sessions_avg_daily,\n  usage_avg_daily,\n  (sessions_30 / NULLIF(days_active_30, 0)) AS sessions_per_active_day,\n  (usage_30 / NULLIF(days_active_30, 0)) AS usage_per_active_day,\n  (tickets_30 / NULLIF(sessions_30, 0)) * 100.0 AS tickets_per_100_sessions,\n  (usage_30 / NULLIF(sessions_30, 0)) AS usage_per_session,\n  churn_30d\nFROM agg;\n'

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    raw_path = Path(cfg["raw_path"])
    feat_path = Path(cfg["features_path"])
    ensure_dir(feat_path.parent)

    df = pd.read_csv(raw_path)

    con = duckdb.connect(database=":memory:")
    con.register("raw_usage", df)

    features = con.execute(FEATURE_SQL).fetchdf()

    # Basic guardrails (engineer-friendly)
    if features.isna().mean().max() > 0.05:
        raise ValueError("Feature null rate too high — check feature logic or raw data quality.")
    if features["user_id"].duplicated().any():
        raise ValueError("User-level features must be unique per user_id.")

    features.to_parquet(feat_path, index=False)
    print(f"Wrote features: {feat_path.resolve()} ({len(features):,} users)")

if __name__ == "__main__":
    main()
