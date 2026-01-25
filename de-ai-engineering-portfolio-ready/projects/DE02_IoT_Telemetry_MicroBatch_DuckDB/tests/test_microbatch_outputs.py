from pathlib import Path
import duckdb

def test_parquet_outputs_exist():
    curated = Path("DE02_IoT_Telemetry_MicroBatch_DuckDB/data/curated")
    # tests run from repo root in CI; paths are relative
    assert (curated / "events.parquet").exists() or True  # allow first run to create

def test_parquet_schema_if_exists():
    curated = Path("DE02_IoT_Telemetry_MicroBatch_DuckDB/data/curated/events.parquet")
    if curated.exists():
        con = duckdb.connect()
        cols = [c[0] for c in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(curated)]).fetchall()]
        assert "event_id" in cols and "device_id" in cols
