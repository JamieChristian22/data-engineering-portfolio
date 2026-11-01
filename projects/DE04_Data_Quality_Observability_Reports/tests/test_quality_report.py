"""Verify DE04 quality controls rather than merely checking file existence."""
import json
from pathlib import Path
import pytest

PROJECT = Path(__file__).resolve().parents[1]

def test_quality_report_has_executed_checks():
    report = PROJECT / "outputs/quality_report.json"
    if not report.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE04 to generate the report")
    data = json.loads(report.read_text())
    assert data["metrics"]["rows"] > 0
    checks = {item["name"]: item for item in data["checks"]}
    assert {"schema_expected_columns", "order_id_unique", "max_null_rate_under_threshold",
            "row_count_in_range", "refund_rate_under_threshold",
            "freshness_within_sla_hours"}.issubset(checks)
    assert data["overall_pass"] == all(item["pass"] for item in checks.values())
    assert all(isinstance(item["pass"], bool) for item in checks.values())

def test_quality_html_exists_with_report():
    report = PROJECT / "outputs/quality_report.json"
    html = PROJECT / "outputs/quality_report.html"
    if not report.exists() or not html.exists():
        pytest.skip("Run bash run_demo_all.sh --only DE04 to generate both reports")
    page = html.read_text(encoding="utf-8")
    assert "<h1>Data Quality Report</h1>" in page
    assert "schema_expected_columns" in page
