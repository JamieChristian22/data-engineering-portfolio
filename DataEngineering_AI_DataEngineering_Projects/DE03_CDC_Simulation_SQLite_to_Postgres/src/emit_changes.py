from __future__ import annotations
import argparse, sqlite3, json, random
from pathlib import Path
from datetime import datetime, timezone
from shared.utils import utcnow_iso

def emit(conn: sqlite3.Connection, table: str, op: str, pk: str, payload: dict) -> None:
    conn.execute(
        "INSERT INTO cdc_log(table_name, op, pk, payload_json, emitted_at) VALUES (?,?,?,?,?)",
        (table, op, pk, json.dumps(payload), utcnow_iso())
    )

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches", type=int, default=3)
    args = ap.parse_args()

    cfg = json.loads(Path("config/config.json").read_text())
    conn = sqlite3.connect(cfg["sqlite_path"])
    conn.row_factory = sqlite3.Row

    random.seed(9)

    for b in range(args.batches):
        # Randomly update a user country or soft-delete
        user_id = random.choice([1,2,3,4])
        if random.random() < 0.2:
            conn.execute("UPDATE users SET is_deleted=1 WHERE user_id=?", (user_id,))
            row = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone())
            emit(conn, "users", "D", str(user_id), row)
        else:
            country = random.choice(["US","CA","GB","AU","DE"])
            conn.execute("UPDATE users SET country=? WHERE user_id=?", (country, user_id))
            row = dict(conn.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone())
            emit(conn, "users", "U", str(user_id), row)

        # Insert a new subscription sometimes
        if random.random() < 0.6:
            sub_id = random.randint(200, 999)
            plan = random.choice(["basic","pro","enterprise"])
            mrr = {"basic":19.0,"pro":49.0,"enterprise":199.0}[plan]
            status = random.choice(["active","active","trial"])
            payload = {
                "sub_id": sub_id,
                "user_id": user_id,
                "plan": plan,
                "status": status,
                "mrr_usd": mrr,
                "started_at": utcnow_iso(),
                "ended_at": None
            }
            conn.execute(
                "INSERT OR REPLACE INTO subscriptions(sub_id,user_id,plan,status,mrr_usd,started_at,ended_at) VALUES (?,?,?,?,?,?,?)",
                (payload["sub_id"], payload["user_id"], payload["plan"], payload["status"], payload["mrr_usd"], payload["started_at"], payload["ended_at"])
            )
            emit(conn, "subscriptions", "I", str(sub_id), payload)

        conn.commit()
        print(f"Emitted batch {b+1}/{args.batches}")

    conn.close()

if __name__ == "__main__":
    main()
