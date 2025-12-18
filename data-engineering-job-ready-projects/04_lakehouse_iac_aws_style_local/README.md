# 04 — Lakehouse Pipeline + IaC (AWS-style) with DuckDB + Parquet + Terraform

## What this project demonstrates
- A **lakehouse directory structure** (bronze/silver/gold) with parquet data.
- A fully working **local pipeline** using DuckDB (fast, portable, great for demos).
- **Infrastructure-as-code** (Terraform) that mirrors a real AWS setup (S3 + Glue + Athena).

## Architecture (Mermaid)
```mermaid
flowchart TB
  L[App JSON logs] --> B[Bronze Parquet]
  B --> S[Silver Parquet: cleaned]
  S --> G[Gold Parquet: marts]
  G --> Q[Analytics SQL]
  subgraph IaC (optional)
    T[Terraform] --> S3[(S3 Lake)]
    T --> Glue[Glue Catalog DB]
    T --> Athena[Athena WG]
  end
```

## Run locally
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python run_lakehouse_pipeline.py
```

Outputs:
- `lakehouse/bronze/app_events.parquet`
- `lakehouse/silver/app_events_clean.parquet`
- `lakehouse/gold/mart_actions_daily.parquet`

## What to screenshot for proof
- Script output showing bronze/silver/gold paths
- Sample query dataframe printed at the end
- The lakehouse folder with generated parquet files
