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

## Quickstart
1) Start Postgres (optional)
```bash
cd docker
docker compose up -d
```

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
- All credentials are local defaults in `config/config.json` and the docker compose.


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
