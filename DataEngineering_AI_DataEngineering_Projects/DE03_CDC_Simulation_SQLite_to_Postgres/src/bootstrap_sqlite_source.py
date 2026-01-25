from __future__ import annotations
import sqlite3, json
from pathlib import Path
from datetime import datetime, timezone
from shared.utils import ensure_dir, utcnow_iso

DDL = [
"""CREATE TABLE IF NOT EXISTS users(
  user_id INTEGER PRIMARY KEY,
  email TEXT NOT NULL,
  country TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_deleted INTEGER NOT NULL DEFAULT 0
);""",
"""CREATE TABLE IF NOT EXISTS subscriptions(
  sub_id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  plan TEXT NOT NULL,
  status TEXT NOT NULL,
  mrr_usd REAL NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT
);""",
"""CREATE TABLE IF NOT EXISTS cdc_log(
  cdc_id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_name TEXT NOT NULL,
  op TEXT NOT NULL, -- I/U/D
  pk TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  emitted_at TEXT NOT NULL
);"""
]

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    db_path = Path(cfg["sqlite_path"])
    ensure_dir(db_path.parent)
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    for stmt in DDL:
        cur.execute(stmt)

    # Seed baseline users + subscriptions
    now = utcnow_iso()
    users = [
        (1, "alice@example.com", "US", now, 0),
        (2, "bob@example.com", "CA", now, 0),
        (3, "carmen@example.com", "US", now, 0),
        (4, "derek@example.com", "GB", now, 0),
    ]
    cur.executemany("INSERT OR REPLACE INTO users VALUES (?,?,?,?,?)", users)

    subs = [
        (101, 1, "pro", "active", 49.0, now, None),
        (102, 2, "basic", "active", 19.0, now, None),
        (103, 3, "pro", "canceled", 49.0, now, now),
    ]
    cur.executemany("INSERT OR REPLACE INTO subscriptions VALUES (?,?,?,?,?,?,?)", subs)
    conn.commit()
    conn.close()
    print(f"Bootstrapped SQLite source at {db_path.resolve()}")

if __name__ == "__main__":
    main()
