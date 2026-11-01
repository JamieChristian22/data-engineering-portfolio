# Data Engineering & AI Pipeline Portfolio

**Python · SQL · DuckDB · PostgreSQL · dbt · Data Quality · Machine Learning**

[![Data pipeline CI](https://github.com/JamieChristian22/data-engineering-portfolio/actions/workflows/data-pipelines.yml/badge.svg)](https://github.com/JamieChristian22/data-engineering-portfolio/actions/workflows/data-pipelines.yml)
![Python 3.11](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)
![Offline labs](https://img.shields.io/badge/Offline_demo-6_projects-146A77)
[![GitHub Sponsors](https://img.shields.io/badge/Sponsor-GitHub_Sponsors-EA4AAA?logo=githubsponsors)](https://github.com/sponsors/JamieChristian22)

A hands-on portfolio demonstrating the design, execution and validation of simulated data pipelines: from raw data ingestion to transformation, quality controls, feature engineering and monitoring.

**Evidence boundary:** All examples use synthetic, generated or modeled data. Passing local tests is evidence of scripted behavior, **not** production cloud deployment, real business impact, or accuracy on unseen real-world datasets.

## Start here

| What to inspect | Link | Why |
|---|---|---|
| Offline six-pipeline runner | [run_demo_all.sh](run_demo_all.sh) | Single reproducible command, safe to launch from another directory |
| Validation evidence | [scripts/verify_outputs.py](scripts/verify_outputs.py) | Requires actual JSON output from each pipeline and summarizes results |
| Interactive pipeline explorer | [demo/pipeline-explorer.html](demo/pipeline-explorer.html) | Load a local validation JSON and explore project statuses |
| Automated CI | [.github/workflows/data-pipelines.yml](.github/workflows/data-pipelines.yml) | Tests, real demo execution and secret scanning |
| Project source index | [projects/README.md](projects/README.md) | Seven documented projects, six executable without cloud or database accounts |
| Quality and security notes | [docs/UPGRADE-VALIDATION.md](docs/UPGRADE-VALIDATION.md) | Scope, evidence limits and validation commands |

## Project index

| ID | Project | Offline demo | Key outputs |
|---|---|---|---|
| [DE01](projects/DE01_Retail_Orders_ELT_dbt_Postgres/) | Retail Orders ELT / Postgres / dbt | Separate setup required | dbt models, KPI exports |
| [DE02](projects/DE02_IoT_Telemetry_MicroBatch_DuckDB/) | IoT Micro-Batch / Parquet | Yes | Curated events, device rollups |
| [DE03](projects/DE03_CDC_Simulation_SQLite_to_Postgres/) | CDC / SQLite → DuckDB | Yes | Warehouse tables, checkpoint |
| [DE04](projects/DE04_Data_Quality_Observability_Reports/) | Data Quality & Observability | Yes | JSON/HTML quality results |
| [AI01](projects/AI01_Document_Ingestion_Vector_Index/) | Document Vector Search | Yes | Local index, query results |
| [AI02](projects/AI02_Churn_Feature_Pipeline_Training_Monitoring/) | Churn Features and Model Monitoring | Yes | Model scores, drift report |
| [AI03](projects/AI03_Transaction_Anomaly_Scoring_Pipeline/) | Transaction Anomaly Detection | Yes | Scored transactions, alert monitoring |

## Quickstart — no cloud credentials

Requires Python 3.11, Bash, and internet access to install the initial Python dependencies.

```bash
git clone https://github.com/JamieChristian22/data-engineering-portfolio.git
cd data-engineering-portfolio
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

# Show offline options (DE01 needs a separate local database)
bash run_demo_all.sh --list

# Run all six offline demos
bash run_demo_all.sh

# Check output contracts and generate real evidence
python scripts/verify_outputs.py --output demo/latest-run.json

# Unit/regression and generated-artifact checks
python -m pytest -q
```

On Windows, use a Bash-compatible terminal for the runner and `.venv\Scripts\activate` to enable the environment in PowerShell. Python's `PYTHONPATH` is exported automatically by the Bash runner.

Run a single project: `bash run_demo_all.sh --only DE04`, or repeat `--only` for multiple projects. The runner resolves its repository location automatically; you do not need to run it from the root.

### Interactive proof of execution

Open [demo/pipeline-explorer.html](demo/pipeline-explorer.html) locally in a browser and choose `demo/latest-run.json` using the file picker. The app works without a server or API and displays the validation results **only after** you load your locally generated report. No data is transmitted.

[See screenshot-independent review and limits](docs/UPGRADE-VALIDATION.md).

## Understanding the quality alerts

The DE04 fixture intentionally generates rows with historical timestamps (December 2025). Freshness-SLA checks may fail when run later, including in 2026. **This is an expected data-quality finding, not a passed check.** The CLI still emits its JSON/HTML evidence so reviewers can inspect which controls failed. The output verifier checks the report's integrity and preserves the failing results, rather than misrepresenting them as passing.

Likewise, model drift and transaction alert guardrails may identify synthetic conditions. Output-contract validation means evidence exists and has the expected structure, not that every underlying business or security threshold passed.

## DE01 PostgreSQL + dbt (separate environment)

DE01 is deliberately excluded from credential-free `run_demo_all.sh`. It requires a local PostgreSQL container and dbt installation. For local-only experimentation, set a unique development password in `DE01_POSTGRES_PASSWORD`; no password is checked into current configuration files, and container ports bind to localhost. See [DE01 setup](projects/DE01_Retail_Orders_ELT_dbt_Postgres/README.md). Never reuse demonstration passwords or deploy these examples unchanged to public infrastructure.

## Engineering practices shown

Reproducible generation with fixed random seeds; incremental CDC and checkpointing; telemetry deduplication and 5-minute aggregations; schema, null, uniqueness, volume and freshness checks; SQL/dbt analytical models; data and concept-drift reports; artifact-contract validation; GitHub Actions CI and history-based secret detection.

**Validation limitations:** Offline tests do not verify a hosted data warehouse, live streaming infrastructure, enterprise-grade security controls, or real production model performance. Check [the actual workflow results](https://github.com/JamieChristian22/data-engineering-portfolio/actions/workflows/data-pipelines.yml) before claiming a particular revision passed CI.

## About

Portfolio maintained by Jamie Christian II. [LinkedIn](https://www.linkedin.com/in/jamiechristian2/) · [GitHub](https://github.com/JamieChristian22)

If these reproducible portfolio examples are useful, [GitHub Sponsors](https://github.com/sponsors/JamieChristian22) offers optional support for further development. Sponsorship does not affect technical findings or guarantee changes.
