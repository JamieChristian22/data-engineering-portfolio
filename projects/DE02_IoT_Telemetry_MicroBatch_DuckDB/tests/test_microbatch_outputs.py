"""Validate output of the DE02 offline demo, when generated."""
import json
from pathlib import Path
import duckdb
import pytest

PROJECT = Path(__file__).resolve().parents[1]
CURATED = PROJECT / "data/curated"

def test_curated_telemetry_has_expected_schema():
    path = CURATED / "events.parquet"
    if not path.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE02 to generate telemetry")
    con = duckdb.connect()
    try:
        columns = {row[0] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
        assert {"event_id", "device_id", "event_time_utc", "temperature_c"}.issubset(columns)
        assert con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0] > 0
    finally:
        con.close()

def test_rollup_matches_telemetry_and_run_log():
    path = CURATED / "device_5min_rollups.parquet"
    log = PROJECT / "outputs/etl_run_log.json"
    if not path.exists() or not log.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE02 to generate rollups")
    data = json.loads(log.read_text())
    con = duckdb.connect()
    try:
        count = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        assert count > 0
        assert count == data["rollup_rows"]
    finally:
        con.close()
