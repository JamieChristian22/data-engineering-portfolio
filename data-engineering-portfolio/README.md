# Data Engineering + AI Data Engineering Portfolio (7 Real-World Projects)

This package contains:
- **4 Data Engineering pipeline projects** (batch, ELT, CDC-simulation, data quality/observability)
- **3 AI Data Engineering projects** (vector search ingestion, feature pipeline + training + monitoring, anomaly scoring + drift)

All projects are **fully filled-in** (code + SQL + sample data + run steps).  
They’re designed to be runnable locally using **Python + SQL + DuckDB/Postgres + Docker** (optional), and include **tests + CI**.

> Date: 2026-01-25

---

## Projects

### Data Engineering (4)
1. **DE01_Retail_Orders_ELT_dbt_Postgres**
   - ELT into Postgres warehouse + dbt models (staging → marts), incremental loads, SCD Type 2 dim_customer
2. **DE02_IoT_Telemetry_MicroBatch_DuckDB**
   - Micro-batch ingestion of JSON telemetry, aggregations, parquet outputs, late-arrival handling
3. **DE03_CDC_Simulation_SQLite_to_Postgres**
   - Simulated CDC (append-only change log) from SQLite app DB into Postgres warehouse with idempotent upserts
4. **DE04_Data_Quality_Observability_Reports**
   - Data quality checks, anomaly thresholds, freshness checks, and HTML/JSON reporting + pytest

### AI Data Engineering (3)
5. **AI01_Document_Ingestion_Vector_Index**
   - Chunking + metadata + vector indexing (HashingVectorizer) into SQLite, fast semantic-ish search (cosine)
6. **AI02_Churn_Feature_Pipeline_Training_Monitoring**
   - Feature engineering, train model (sklearn), batch scoring, drift checks (KS test), metrics logs
7. **AI03_Transaction_Anomaly_Scoring_Pipeline**
   - IsolationForest anomaly scoring, scheduled batch scoring, alert rules, drift & stability monitoring

---

## Common Tooling

### Python setup (recommended)
From the package root:

```bash
python -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

### Optional: Docker Postgres
Several projects include a `docker-compose.yml` you can run:

```bash
docker compose up -d
```

---

## Quality & CI
A ready-to-use GitHub Actions workflow is included in `.github/workflows/ci.yml` to run:
- unit tests (`pytest`)
- linting (`ruff`) if installed

---

## License
MIT — use freely in portfolios and interviews.
