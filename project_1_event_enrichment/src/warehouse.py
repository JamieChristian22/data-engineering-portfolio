
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import pandas as pd

from .logutil import get_logger
from .validate import require_columns, require_nonnull, require_unique

log = get_logger("p1.warehouse")

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  user_id INTEGER NOT NULL,
  session_id INTEGER NOT NULL,
  event_time TEXT NOT NULL,
  event_type TEXT NOT NULL,
  device TEXT NOT NULL,
  referrer TEXT NOT NULL,
  country TEXT NOT NULL,
  event_index INTEGER NOT NULL,
  propensity_score REAL
);

CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_time);

CREATE TABLE IF NOT EXISTS sessions (
  session_id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  device TEXT NOT NULL,
  referrer TEXT NOT NULL,
  country TEXT NOT NULL,
  n_events INTEGER NOT NULL,
  n_add_to_cart INTEGER NOT NULL,
  n_checkout INTEGER NOT NULL,
  n_purchase INTEGER NOT NULL,
  session_seconds REAL NOT NULL,
  events_per_min REAL NOT NULL,
  propensity_score REAL NOT NULL
);
"""


def load_sqlite(db_path: Path, enriched_events_csv: Path, session_features_csv: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA)

        # Idempotent load: clear tables each run (safe for demo pipelines)
        conn.execute('DELETE FROM events')
        conn.execute('DELETE FROM sessions')
        conn.commit()

        ev = pd.read_csv(enriched_events_csv)
        require_columns(ev, ["event_id","session_id","user_id","event_time","event_type","propensity_score"])
        require_nonnull(ev, ["event_id","session_id","user_id","event_time","event_type"])
        require_unique(ev, "event_id")
        ev.to_sql("events", conn, if_exists="append", index=False)

        sess = pd.read_csv(session_features_csv)
        require_columns(sess, ["session_id","user_id","device","referrer","country","n_events","n_add_to_cart","n_checkout","n_purchase","session_seconds","events_per_min"])
        require_nonnull(sess, ["session_id","user_id","device","referrer","country"])
        # join session-level score from enriched events
        score_map = ev.groupby("session_id")["propensity_score"].mean().reset_index()
        sess = sess.merge(score_map, on="session_id", how="left")
        sess = sess.rename(columns={"n_add_to_cart":"n_add_to_cart","n_checkout":"n_checkout","n_purchase":"n_purchase"})
        sess_cols = ["session_id","user_id","device","referrer","country","n_events","n_add_to_cart","n_checkout","n_purchase","session_seconds","events_per_min","propensity_score"]
        sess[sess_cols].to_sql("sessions", conn, if_exists="append", index=False)

        log.info("Loaded events=%s sessions=%s into %s", len(ev), len(sess), db_path)
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db_path", type=str, default="reports/warehouse.sqlite")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    enriched = root / "data" / "processed" / "enriched_events.csv"
    sess = root / "data" / "processed" / "session_features.csv"

    if not enriched.exists():
        raise FileNotFoundError(f"Missing {enriched}. Run: python -m src.enrich_stream")
    if not sess.exists():
        raise FileNotFoundError(f"Missing {sess}. Run: python -m src.train_model (builds features)")

    load_sqlite(Path(args.db_path), enriched, sess)


if __name__ == "__main__":
    main()
