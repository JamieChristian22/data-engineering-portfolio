from __future__ import annotations
import json, sqlite3
from pathlib import Path
import duckdb
from shared.utils import ensure_dir, utcnow_iso

DDL = [
"""CREATE TABLE IF NOT EXISTS dim_user(
  user_id INTEGER PRIMARY KEY,
  email VARCHAR,
  country VARCHAR,
  created_at TIMESTAMP,
  is_deleted BOOLEAN
);""",
"""CREATE TABLE IF NOT EXISTS fct_subscription(
  sub_id INTEGER PRIMARY KEY,
  user_id INTEGER,
  plan VARCHAR,
  status VARCHAR,
  mrr_usd DOUBLE,
  started_at TIMESTAMP,
  ended_at TIMESTAMP
);""",
"""CREATE TABLE IF NOT EXISTS cdc_audit(
  cdc_id BIGINT,
  table_name VARCHAR,
  op VARCHAR,
  pk VARCHAR,
  applied_at TIMESTAMP
);"""
]

def load_checkpoint(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"last_cdc_id": 0}

def save_checkpoint(path: Path, ckpt: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(ckpt, indent=2))

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    ckpt_path = Path(cfg["checkpoint_file"])
    report_path = ensure_dir(Path(cfg["report_file"]).parent) / Path(cfg["report_file"]).name
    ckpt = load_checkpoint(ckpt_path)

    # Read new CDC rows
    sconn = sqlite3.connect(cfg["sqlite_path"])
    sconn.row_factory = sqlite3.Row
    rows = sconn.execute(
        "SELECT * FROM cdc_log WHERE cdc_id > ? ORDER BY cdc_id",
        (ckpt["last_cdc_id"],)
    ).fetchall()

    dconn = duckdb.connect(cfg["duckdb_path"])
    for ddl in DDL:
        dconn.execute(ddl)

    applied = 0
    for r in rows:
        payload = json.loads(r["payload_json"])
        if r["table_name"] == "users":
            # upsert
            dconn.execute(
                """INSERT INTO dim_user(user_id,email,country,created_at,is_deleted)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(user_id) DO UPDATE SET
                     email=excluded.email,
                     country=excluded.country,
                     created_at=excluded.created_at,
                     is_deleted=excluded.is_deleted
                """,
                [payload["user_id"], payload["email"], payload["country"], payload["created_at"], bool(payload["is_deleted"])]
            )
        elif r["table_name"] == "subscriptions":
            dconn.execute(
                """INSERT INTO fct_subscription(sub_id,user_id,plan,status,mrr_usd,started_at,ended_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(sub_id) DO UPDATE SET
                     user_id=excluded.user_id,
                     plan=excluded.plan,
                     status=excluded.status,
                     mrr_usd=excluded.mrr_usd,
                     started_at=excluded.started_at,
                     ended_at=excluded.ended_at
                """,
                [payload["sub_id"], payload["user_id"], payload["plan"], payload["status"], payload["mrr_usd"], payload["started_at"], payload["ended_at"]]
            )
        dconn.execute(
            "INSERT INTO cdc_audit(cdc_id,table_name,op,pk,applied_at) VALUES (?,?,?,?,?)",
            [r["cdc_id"], r["table_name"], r["op"], r["pk"], utcnow_iso()]
        )
        applied += 1
        ckpt["last_cdc_id"] = int(r["cdc_id"])

    save_checkpoint(ckpt_path, ckpt)
    report = {
        "run_at": utcnow_iso(),
        "applied_rows": applied,
        "new_last_cdc_id": ckpt["last_cdc_id"],
        "dim_user_rows": int(dconn.execute("SELECT COUNT(*) FROM dim_user").fetchone()[0]),
        "fct_subscription_rows": int(dconn.execute("SELECT COUNT(*) FROM fct_subscription").fetchone()[0]),
    }
    report_path.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))

    dconn.close()
    sconn.close()

if __name__ == "__main__":
    main()
