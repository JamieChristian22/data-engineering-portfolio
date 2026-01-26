import json
from pathlib import Path
import duckdb

def test_checkpoint_schema():
    ckpt = Path("DE03_CDC_Simulation_SQLite_to_Postgres/state/checkpoint.json")
    if ckpt.exists():
        data = json.loads(ckpt.read_text())
        assert "last_cdc_id" in data

def test_duckdb_tables_if_exist():
    db = Path("DE03_CDC_Simulation_SQLite_to_Postgres/warehouse/warehouse.duckdb")
    if db.exists():
        con = duckdb.connect(str(db))
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert "dim_user" in tables and "fct_subscription" in tables
        con.close()
