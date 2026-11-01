#!/usr/bin/env bash
# Offline demos only. DE01 requires a separate local Postgres/dbt environment.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
PYTHON="${PYTHON:-python3}"

usage() {
  echo "Usage: bash run_demo_all.sh [--list] [--only DE02|DE03|DE04|AI01|AI02|AI03]..."
  echo "With no arguments, runs all six credential-free demos."
}
list() {
  printf '%s\n' "DE02 IoT telemetry micro-batch" "DE03 SQLite to DuckDB CDC" \
    "DE04 Data quality and observability" "AI01 Document vector search" \
    "AI02 Churn model and drift" "AI03 Anomaly scoring and monitoring"
  echo "DE01 Postgres/dbt is excluded: requires a separate local database."
}
run_step() { "$PYTHON" "$@"; }
run_project() {
  local code="$1" folder
  case "$code" in
    DE02) folder="DE02_IoT_Telemetry_MicroBatch_DuckDB" ;;
    DE03) folder="DE03_CDC_Simulation_SQLite_to_Postgres" ;;
    DE04) folder="DE04_Data_Quality_Observability_Reports" ;;
    AI01) folder="AI01_Document_Ingestion_Vector_Index" ;;
    AI02) folder="AI02_Churn_Feature_Pipeline_Training_Monitoring" ;;
    AI03) folder="AI03_Transaction_Anomaly_Scoring_Pipeline" ;;
    *) echo "Unknown demo: $code" >&2; usage >&2; return 2 ;;
  esac
  echo "== $code / $folder =="
  (
    cd "$ROOT/projects/$folder"
    case "$code" in
      DE02)
        run_step src/generate_stream.py --minutes 1 --devices 12
        run_step src/microbatch_etl.py
        ;;
      DE03)
        run_step src/bootstrap_sqlite_source.py
        run_step src/emit_changes.py --batches 5
        run_step src/replicate_to_duckdb_warehouse.py
        ;;
      DE04)
        run_step src/generate_daily_fact_data.py
        run_step src/run_quality_suite.py
        ;;
      AI01)
        run_step src/seed_documents.py
        run_step src/build_index.py
        run_step src/search.py --query "late arrivals buffered events" --topk 3
        ;;
      AI02)
        run_step src/generate_raw_usage_data.py
        run_step src/build_features.py
        run_step src/train_model.py
        run_step src/batch_score.py
        run_step src/monitor_drift.py
        ;;
      AI03)
        run_step src/generate_transactions.py
        run_step src/train_anomaly_model.py
        run_step src/score_transactions.py
        run_step src/monitor_pipeline.py
        ;;
    esac
  )
}
selected=()
if [ "$#" -eq 0 ]; then
  selected=(DE02 DE03 DE04 AI01 AI02 AI03)
fi
while [ "$#" -gt 0 ]; do
  case "$1" in
    --list) if [ "$#" -ne 1 ]; then usage >&2; exit 2; fi; list; exit 0 ;;
    --only)
      if [ "$#" -lt 2 ]; then usage >&2; exit 2; fi
      case "$2" in DE02|DE03|DE04|AI01|AI02|AI03) selected+=("$2");; *) echo "Unknown demo: $2" >&2; exit 2;; esac
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
for code in "${selected[@]}"; do
  run_project "$code"
done
echo "Completed ${#selected[@]} credential-free demo(s)."
