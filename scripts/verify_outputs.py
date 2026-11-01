#!/usr/bin/env python3
"""Validate fresh offline demo output contracts; does not require cloud accounts."""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECTS = {
    "DE02": ("DE02_IoT_Telemetry_MicroBatch_DuckDB/outputs/etl_run_log.json",
             ("event_rows_total", "rollup_rows")),
    "DE03": ("DE03_CDC_Simulation_SQLite_to_Postgres/outputs/replication_report.json",
             ("applied_rows", "new_last_cdc_id")),
    "DE04": ("DE04_Data_Quality_Observability_Reports/outputs/quality_report.json",
             ("overall_pass", "checks", "metrics")),
    "AI01": ("AI01_Document_Ingestion_Vector_Index/outputs/search_results.json",
             ("results", "query")),
    "AI02": ("AI02_Churn_Feature_Pipeline_Training_Monitoring/outputs/drift_report.json",
             ("all_features", "flagged_features")),
    "AI03": ("AI03_Transaction_Anomaly_Scoring_Pipeline/outputs/pipeline_monitor_report.json",
             ("alerts", "violations", "volume")),
}

def verify(project_root: Path) -> tuple[dict, bool]:
    entries, valid = [], True
    for code, (relative, required) in PROJECTS.items():
        path = project_root / relative
        entry = {"project": code, "source": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else relative}
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(report, dict):
                raise ValueError("Expected a JSON object")
            missing = [key for key in required if key not in report]
            if missing:
                raise ValueError("Missing fields: " + ", ".join(missing))
            if code == "DE02" and (report["event_rows_total"] <= 0 or report["rollup_rows"] <= 0):
                raise ValueError("Empty telemetry output")
            if code == "DE03" and report["new_last_cdc_id"] <= 0:
                raise ValueError("CDC checkpoint not advanced")
            if code == "DE04" and (report["metrics"]["rows"] <= 0 or not report["checks"]):
                raise ValueError("Missing quality-check results")
            if code == "AI01" and not report["results"]:
                raise ValueError("Empty document search results")
            if code == "AI02" and not report["all_features"]:
                raise ValueError("Missing model drift metrics")
            if code == "AI03" and report["volume"] <= 0:
                raise ValueError("No transaction monitoring rows")
            entry["status"] = "VALIDATED"
            if code == "DE04":
                entry["quality_checks_passed"] = sum(bool(item["pass"]) for item in report["checks"])
                entry["quality_checks_total"] = len(report["checks"])
                entry["quality_overall_pass"] = bool(report["overall_pass"])
                entry["note"] = "Historic fixture may intentionally fail freshness SLA"
            elif code == "AI03":
                entry["alerts"] = int(report["alerts"])
                entry["violations"] = list(report["violations"])
            elif code == "AI01":
                entry["results_returned"] = len(report["results"])
            elif code == "AI02":
                entry["features_examined"] = len(report["all_features"])
                entry["drift_flags"] = len(report["flagged_features"])
            elif code == "DE02":
                entry["events"] = int(report["event_rows_total"])
                entry["rollups"] = int(report["rollup_rows"])
            else:
                entry["cdc_events_applied"] = int(report["applied_rows"])
        except (OSError, ValueError, TypeError, KeyError) as error:
            entry.update(status="ERROR", error=str(error))
            valid = False
        entries.append(entry)
    return {"schema_version": 1,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "scope": "Synthetic local projects. DE01 needs separate Postgres/dbt setup.",
            "all_output_contracts_valid": valid,
            "projects": entries}, valid

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="demo/latest-run.json")
    args = parser.parse_args()
    summary, passed = verify(ROOT / "projects")
    destination = Path(args.output)
    if not destination.is_absolute():
        destination = ROOT / destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    for item in summary["projects"]:
        print(item["project"], item["status"], item.get("error", ""))
    print("Saved summary:", destination)
    return 0 if passed else 1

if __name__ == "__main__":
    raise SystemExit(main())
