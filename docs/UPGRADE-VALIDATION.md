# Upgrade and Validation Guide — Data Engineering Portfolio

## Scope

This upgrade adds a reliable six-pipeline offline launcher, project-level regression tests, a generated-output verifier, CI, a browser-based pipeline explorer and safer DE01 local database defaults. It does not deploy any cloud services, run dbt against a managed warehouse, or claim production correctness.

## Reproduce locally

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
bash -n run_demo_all.sh
python -m compileall -q projects shared scripts tests
bash run_demo_all.sh
python scripts/verify_outputs.py --output demo/latest-run.json
python -m pytest -q
```

Run one demo independently: `bash run_demo_all.sh --only DE02`. The script locates `projects/` based on its own file path and exports `PYTHONPATH` so shared library imports work regardless of the invoking directory.

## Output evidence map

| Code | Source report | Example checks |
|---|---|---|
| DE02 | `projects/DE02_IoT_Telemetry_MicroBatch_DuckDB/outputs/etl_run_log.json` | Nonzero events and rollups, Parquet schema |
| DE03 | `projects/DE03_CDC_Simulation_SQLite_to_Postgres/outputs/replication_report.json` | Checkpoint and warehouse row counts |
| DE04 | `projects/DE04_Data_Quality_Observability_Reports/outputs/quality_report.json` | All six named quality checks, including historical freshness failures |
| AI01 | `projects/AI01_Document_Ingestion_Vector_Index/outputs/search_results.json` | Nonempty local vector search result |
| AI02 | `projects/AI02_Churn_Feature_Pipeline_Training_Monitoring/outputs/drift_report.json` | Drift metrics for examined model features |
| AI03 | `projects/AI03_Transaction_Anomaly_Scoring_Pipeline/outputs/pipeline_monitor_report.json` | Processed volume and alert guardrail results |

`scripts/verify_outputs.py` reads these files and writes `demo/latest-run.json` containing measured counts and per-project contract status. It returns a nonzero status if any artifact is absent, malformed, or violates a required minimum. The verifier **does not** mark DE04's intentionally stale data as fresh or convert guardrail warnings into successful quality checks.

## Interactive reviewer demonstration

Open `demo/pipeline-explorer.html` in a browser; use the file picker to load `demo/latest-run.json` created by your own execution. Filter by data/AI category and validated/error/pending status. No CDN, external API, telemetry transmission or credentials are used. When a report has not been loaded, the UI displays "not run / no evidence" instead of fabricated metrics.

## GitHub Actions behavior

`.github/workflows/data-pipelines.yml` uses read-only repository permissions with SHA-pinned actions:

- `offline-pipelines`: installs Python dependencies, validates syntax, runs all six offline demos, checks actual JSON output contracts, runs pytest, and uploads an evidence artifact even if the job fails.
- `secrets`: downloads a SHA256-verified Gitleaks binary and scans repository Git history for recognizable credentials.

Review the [actual CI runs](https://github.com/JamieChristian22/data-engineering-portfolio/actions/workflows/data-pipelines.yml) before making any hosted-success claim. Absence of secret scanner findings is not a proof that all sensitive information is absent.

## Local PostgreSQL safety

DE01 requires explicit `DE01_POSTGRES_PASSWORD` supplied outside version control. Docker Compose binds PostgreSQL and Adminer to `127.0.0.1` only. No examples should be deployed unchanged to public infrastructure. Example passwords from prior Git history should never be reused as real credentials.

## Follow-up improvements

After CI has passed, prioritize a database-backed DE01 integration job with disposable containers; container-image and dependency vulnerability scanning; synthetic schema evolution checks; stable dependency version ranges and tested Python versions; improved deterministic anomaly reporting; and optional hosted read-only demos.
