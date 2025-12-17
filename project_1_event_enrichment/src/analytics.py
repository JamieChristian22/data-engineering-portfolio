
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

from .logutil import get_logger

log = get_logger("p1.analytics")

QUERIES = {
  "conversion_by_device": """
    SELECT device,
           COUNT(*) AS sessions,
           SUM(CASE WHEN n_purchase > 0 THEN 1 ELSE 0 END) AS purchases,
           ROUND(1.0 * SUM(CASE WHEN n_purchase > 0 THEN 1 ELSE 0 END) / COUNT(*), 4) AS conversion_rate
    FROM sessions
    GROUP BY device
    ORDER BY conversion_rate DESC;
  """,
  "conversion_by_referrer": """
    SELECT referrer,
           COUNT(*) AS sessions,
           SUM(CASE WHEN n_purchase > 0 THEN 1 ELSE 0 END) AS purchases,
           ROUND(1.0 * SUM(CASE WHEN n_purchase > 0 THEN 1 ELSE 0 END) / COUNT(*), 4) AS conversion_rate
    FROM sessions
    GROUP BY referrer
    ORDER BY conversion_rate DESC;
  """,
  "score_deciles": """
    WITH ranked AS (
      SELECT *,
             NTILE(10) OVER (ORDER BY propensity_score) AS decile
      FROM sessions
    )
    SELECT decile,
           COUNT(*) AS sessions,
           ROUND(AVG(propensity_score), 4) AS avg_score,
           ROUND(AVG(CASE WHEN n_purchase>0 THEN 1.0 ELSE 0.0 END), 4) AS conversion_rate
    FROM ranked
    GROUP BY decile
    ORDER BY decile;
  """
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db_path", type=str, default="reports/warehouse.sqlite")
    args = ap.parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Missing DB at {db_path}. Run: python -m src.warehouse")

    conn = sqlite3.connect(db_path)
    try:
        outputs = {}
        for name, q in QUERIES.items():
            df = pd.read_sql_query(q, conn)
            outputs[name] = df.to_dict(orient="records")
            log.info("Query %s -> %s rows", name, len(df))

        out_path = db_path.parent / "analytics_summary.json"
        out_path.write_text(json.dumps(outputs, indent=2))
        log.info("Wrote %s", out_path)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
