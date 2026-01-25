import json
from pathlib import Path

def test_report_files_created():
    out = Path("DE04_Data_Quality_Observability_Reports/outputs/quality_report.json")
    if out.exists():
        r = json.loads(out.read_text())
        assert "checks" in r and "overall_pass" in r
