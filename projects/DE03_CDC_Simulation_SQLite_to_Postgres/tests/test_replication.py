"""Validate observed CDC checkpoints and warehouse tables."""
import json
from pathlib import Path
import duckdb
import pytest

PROJECT = Path(__file__).resolve().parents[1]

def test_cdc_checkpoint_and_report_agree():
    ckpt = PROJECT / "state/checkpoint.json"
    report = PROJECT / "outputs/replication_report.json"
    if not ckpt.exists() or not report.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE03 to generate CDC output")
    state = json.loads(ckpt.read_text())
    summary = json.loads(report.read_text())
    assert state["last_cdc_id"] > 0
    assert state["last_cdc_id"] == summary["new_last_cdc_id"]

def test_warehouse_counts_agree_with_report():
    db = PROJECT / "warehouse/warehouse.duckdb"
    report = PROJECT / "outputs/replication_report.json"
    if not db.exists() or not report.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE03 to generate warehouse")
    summary = json.loads(report.read_text())
    con = duckdb.connect(str(db), read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert {"dim_user", "fct_subscription", "cdc_audit"}.issubset(tables)
        assert con.execute("SELECT COUNT(*) FROM dim_user").fetchone()[0] == summary["dim_user_rows"]
        assert con.execute("SELECT COUNT(*) FROM fct_subscription").fetchone()[0] == summary["fct_subscription_rows"]
    finally:
        con.close()
