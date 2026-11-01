# DE01 — Retail Orders ELT (Postgres Warehouse + dbt)

## Business scenario
A retail company sells across **web / mobile / partner** channels. The analytics team needs a warehouse with:
- clean staging tables
- a star schema for BI
- incremental loads
- customer dimension history (SCD Type 2)

## What you’ll build
- Generate raw CSV extracts (simulating daily exports)
- Load into **Postgres** (raw schema)
- Transform with **dbt** (staging → marts)
- Validate with tests and produce KPI outputs

## Tech
- Python (extract/generate + load)
- Postgres (warehouse)
- dbt (models + tests)
- SQL (analytics queries)
- Docker Compose (optional, for local Postgres)

## Quickstart (local-only PostgreSQL)
Start in the DE01 project directory. Choose a unique development-only password and **do not commit it**. These containers bind to `127.0.0.1`, not every network interface.

```bash
export DE01_POSTGRES_PASSWORD='replace-with-a-unique-local-password'
cd docker
docker compose up -d
cd ..
```

Docker and dbt are **optional** for the rest of the portfolio but required for this project. Do not expose the local database or Adminer to the public internet. The same environment variable is consumed by the Python loader, KPI reporting, and dbt profile.

2) Create & load raw data
```bash
python src/generate_raw_data.py
python src/load_raw_to_postgres.py
```

3) Run dbt models (requires dbt-postgres installed)
```bash
cd dbt_retail
dbt deps
dbt seed
dbt run
dbt test
```

4) Run analytics queries
```bash
python src/run_kpis.py
```

## Outputs
- `outputs/kpi_summary.csv`
- `outputs/top_categories_monthly.csv`

## Notes
- No database password is stored in the current committed config. Supply `DE01_POSTGRES_PASSWORD` through the environment for local development. Legacy example values may remain in Git history; do not reuse them as real credentials.
- Containers expose PostgreSQL and Adminer only on the local loopback interface.
- To run dbt from `dbt_retail/`, specify the local profile using `dbt run --profiles-dir .` and `dbt test --profiles-dir .`. Treat production secrets and network configuration as separate concerns.


---
## Executive Summary (Recruiter-Friendly)

**Problem:** Simulated real-world business pipeline challenge  
**Solution:** Designed and implemented a production-style data pipeline  
**Skills Demonstrated:** Data modeling, ETL/ELT, validation, automation, analytics thinking  
**Artifacts Produced:** Reproducible code, real outputs, reports, metrics  
**Interview Talking Points:**
- Why this architecture was chosen
- How idempotency / data quality / monitoring is handled
- How this would scale in production
- Tradeoffs considered in design

## Architecture Diagram
See `architecture.mmd` for a Mermaid-renderable pipeline diagram.

---
