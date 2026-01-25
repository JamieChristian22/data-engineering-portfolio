from __future__ import annotations
import json, sqlite3
from pathlib import Path
from shared.utils import utcnow_iso, ensure_dir
import psycopg2
import psycopg2.extras as extras

DDL = [
    "CREATE SCHEMA IF NOT EXISTS analytics;",
    """CREATE TABLE IF NOT EXISTS analytics.dim_user(
        user_id INT PRIMARY KEY,
        email TEXT,
        country TEXT,
        created_at TIMESTAMP,
        is_deleted BOOLEAN
    );""",
    """CREATE TABLE IF NOT EXISTS analytics.fct_subscription(
        sub_id INT PRIMARY KEY,
        user_id INT,
        plan TEXT,
        status TEXT,
        mrr_usd NUMERIC,
        started_at TIMESTAMP,
        ended_at TIMESTAMP
    );""",
    """CREATE TABLE IF NOT EXISTS analytics.cdc_audit(
        cdc_id BIGINT,
        table_name TEXT,
        op TEXT,
        pk TEXT,
        applied_at TIMESTAMP
    );"""
]

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    pg = cfg.get("postgres", {"host":"localhost","port":5432,"database":"cdc_dw","user":"cdc","password":"cdc"})
    ckpt_path = Path(cfg["checkpoint_file"])
    ckpt = json.loads(ckpt_path.read_text()) if ckpt_path.exists() else {"last_cdc_id": 0}

    sconn = sqlite3.connect(cfg["sqlite_path"])
    sconn.row_factory = sqlite3.Row
    rows = sconn.execute("SELECT * FROM cdc_log WHERE cdc_id > ? ORDER BY cdc_id", (ckpt["last_cdc_id"],)).fetchall()

    pconn = psycopg2.connect(host=pg["host"], port=pg["port"], dbname=pg["database"], user=pg["user"], password=pg["password"])
    pconn.autocommit = True
    cur = pconn.cursor()
    for ddl in DDL:
        cur.execute(ddl)

    for r in rows:
        payload = json.loads(r["payload_json"])
        if r["table_name"] == "users":
            cur.execute(
                """INSERT INTO analytics.dim_user(user_id,email,country,created_at,is_deleted)
                   VALUES (%s,%s,%s,%s,%s)
                   ON CONFLICT(user_id) DO UPDATE SET
                     email=excluded.email,
                     country=excluded.country,
                     created_at=excluded.created_at,
                     is_deleted=excluded.is_deleted""",
                (payload["user_id"], payload["email"], payload["country"], payload["created_at"], bool(payload["is_deleted"]))
            )
        elif r["table_name"] == "subscriptions":
            cur.execute(
                """INSERT INTO analytics.fct_subscription(sub_id,user_id,plan,status,mrr_usd,started_at,ended_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(sub_id) DO UPDATE SET
                     user_id=excluded.user_id,
                     plan=excluded.plan,
                     status=excluded.status,
                     mrr_usd=excluded.mrr_usd,
                     started_at=excluded.started_at,
                     ended_at=excluded.ended_at""",
                (payload["sub_id"], payload["user_id"], payload["plan"], payload["status"], payload["mrr_usd"], payload["started_at"], payload["ended_at"])
            )
        cur.execute("INSERT INTO analytics.cdc_audit(cdc_id,table_name,op,pk,applied_at) VALUES (%s,%s,%s,%s,%s)",
                    (r["cdc_id"], r["table_name"], r["op"], r["pk"], utcnow_iso()))
        ckpt["last_cdc_id"] = int(r["cdc_id"])

    ensure_dir(ckpt_path.parent)
    ckpt_path.write_text(json.dumps(ckpt, indent=2))
    print({"applied": len(rows), "last_cdc_id": ckpt["last_cdc_id"]})

    cur.close(); pconn.close(); sconn.close()

if __name__ == "__main__":
    main()
