from __future__ import annotations

from pathlib import Path
import duckdb

BASE = Path(__file__).resolve().parents[0]
DATA = BASE / "data" / "logs" / "app_events.jsonl"
LAKE = BASE / "lakehouse"
BRONZE = LAKE / "bronze"
SILVER = LAKE / "silver"
GOLD = LAKE / "gold"

for p in [BRONZE, SILVER, GOLD]:
    p.mkdir(parents=True, exist_ok=True)

con = duckdb.connect()

# 1) Bronze: ingest raw jsonl to parquet
bronze_path = BRONZE / "app_events.parquet"
con.execute(f"""
COPY (
  SELECT * FROM read_json_auto('{DATA.as_posix()}')
) TO '{bronze_path.as_posix()}' (FORMAT PARQUET);
""")

# 2) Silver: enforce types + basic cleaning (latency bounds, bytes bounds, timestamps)
silver_path = SILVER / "app_events_clean.parquet"
con.execute(f"""
COPY (
  SELECT
    event_id,
    CAST(ts AS TIMESTAMP) AS ts,
    ip,
    user_id::INTEGER AS user_id,
    action,
    GREATEST(1, LEAST(latency_ms::INTEGER, 5000)) AS latency_ms,
    GREATEST(0, LEAST(bytes::INTEGER, 200000)) AS bytes,
    country
  FROM read_parquet('{bronze_path.as_posix()}')
) TO '{silver_path.as_posix()}' (FORMAT PARQUET);
""")

# 3) Gold: aggregated marts
gold_actions = GOLD / "mart_actions_daily.parquet"
con.execute(f"""
COPY (
  SELECT
    DATE_TRUNC('day', ts)::DATE AS date,
    country,
    action,
    COUNT(*) AS events,
    AVG(latency_ms) AS avg_latency_ms,
    SUM(bytes) AS total_bytes
  FROM read_parquet('{silver_path.as_posix()}')
  GROUP BY 1,2,3
  ORDER BY 1,2,3
) TO '{gold_actions.as_posix()}' (FORMAT PARQUET);
""")

print("✅ Lakehouse pipeline complete.")
print("Bronze:", bronze_path)
print("Silver:", silver_path)
print("Gold:", gold_actions)

# Sample query
df = con.execute(f"SELECT * FROM read_parquet('{gold_actions.as_posix()}') LIMIT 10").df()
print(df)
