from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pandas as pd
from shared.utils import ensure_dir, sha256_file, utcnow_iso

def load_state(state_file: Path) -> dict:
    if state_file.exists():
        return json.loads(state_file.read_text())
    return {"processed": {}}

def save_state(state_file: Path, state: dict) -> None:
    ensure_dir(state_file.parent)
    state_file.write_text(json.dumps(state, indent=2))

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    landing = ensure_dir(Path(cfg["landing_dir"]))
    curated = ensure_dir(Path(cfg["curated_dir"]))
    state_file = Path(cfg["state_file"])
    run_log_path = ensure_dir(Path(cfg["run_log"]).parent) / Path(cfg["run_log"]).name

    state = load_state(state_file)

    files = sorted(landing.glob("*.jsonl"))
    new_files = []
    for f in files:
        h = sha256_file(f)
        if f.name not in state["processed"] or state["processed"][f.name]["sha256"] != h:
            new_files.append((f, h))

    if not new_files:
        print("No new files to process.")
        return

    # DuckDB ETL
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    # Read JSONL into a staging table
    con.execute("""CREATE TABLE stg_events AS
      SELECT * FROM read_json_auto(?, format='newline_delimited');
    """, [str(new_files[0][0])])

    for f, _h in new_files[1:]:
        con.execute("""INSERT INTO stg_events
          SELECT * FROM read_json_auto(?, format='newline_delimited');
        """, [str(f)])

    # Validate + cast
    con.execute("""CREATE OR REPLACE TABLE clean_events AS
      SELECT
        event_id::VARCHAR AS event_id,
        device_id::VARCHAR AS device_id,
        CAST(event_time AS TIMESTAMP) AT TIME ZONE 'UTC' AS event_time_utc,
        CAST(ingest_time AS TIMESTAMP) AT TIME ZONE 'UTC' AS ingest_time_utc,
        CAST(temperature_c AS DOUBLE) AS temperature_c,
        CAST(vibration_rms AS DOUBLE) AS vibration_rms,
        CAST(battery_pct AS INTEGER) AS battery_pct,
        CAST(lat AS DOUBLE) AS lat,
        CAST(lon AS DOUBLE) AS lon
      FROM stg_events
      WHERE event_id IS NOT NULL
        AND device_id IS NOT NULL
        AND temperature_c BETWEEN -40 AND 120
        AND vibration_rms BETWEEN 0 AND 10
        AND battery_pct BETWEEN 0 AND 100;
    """)

    # Deduplicate by event_id (idempotent)
    con.execute("""CREATE OR REPLACE TABLE dedup_events AS
      SELECT *
      FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingest_time_utc DESC) AS rn
        FROM clean_events
      )
      WHERE rn = 1;
    """)

    # Merge into curated events (parquet)
    events_path = curated / "events.parquet"
    if events_path.exists():
        con.execute("CREATE TABLE existing_events AS SELECT * FROM read_parquet(?);", [str(events_path)])
        con.execute("""CREATE TABLE merged_events AS
          SELECT * FROM existing_events
          UNION ALL
          SELECT * FROM dedup_events
          WHERE event_id NOT IN (SELECT event_id FROM existing_events);
        """)
    else:
        con.execute("CREATE TABLE merged_events AS SELECT * FROM dedup_events;")

    con.execute("COPY merged_events TO ? (FORMAT PARQUET);", [str(events_path)])

    # 5-min rollups
    rollup_path = curated / "device_5min_rollups.parquet"
    con.execute("""CREATE OR REPLACE TABLE rollups AS
      SELECT
        device_id,
        date_bin(INTERVAL '5 minutes', event_time_utc, TIMESTAMP '2025-01-01') AS window_start,
        COUNT(*) AS event_count,
        AVG(temperature_c) AS avg_temp_c,
        MAX(vibration_rms) AS max_vibration,
        MIN(battery_pct) AS min_battery
      FROM merged_events
      GROUP BY 1,2;
    """)

    con.execute("COPY rollups TO ? (FORMAT PARQUET);", [str(rollup_path)])

    # Update state + run log
    processed_at = utcnow_iso()
    for f, h in new_files:
        state["processed"][f.name] = {"sha256": h, "processed_at": processed_at}

    save_state(state_file, state)
    run_log = {
        "run_at": processed_at,
        "new_files": [f.name for f, _ in new_files],
        "events_parquet": str(events_path),
        "rollups_parquet": str(rollup_path),
        "event_rows_total": int(con.execute("SELECT COUNT(*) FROM merged_events").fetchone()[0]),
        "rollup_rows": int(con.execute("SELECT COUNT(*) FROM rollups").fetchone()[0]),
    }
    run_log_path.write_text(json.dumps(run_log, indent=2))
    print(json.dumps(run_log, indent=2))

if __name__ == "__main__":
    main()
