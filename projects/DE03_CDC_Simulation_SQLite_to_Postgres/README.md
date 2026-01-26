# DE03 — CDC Simulation (SQLite App DB → Postgres Warehouse)

## Scenario
An operational app uses SQLite (for simplicity) but analytics needs Postgres.
We simulate CDC via an **append-only change log** table in SQLite and replicate changes into Postgres with:
- **idempotent upserts**
- **soft deletes**
- **high-watermark** checkpointing

## Run (local, no Docker required)
```bash
python src/bootstrap_sqlite_source.py
python src/emit_changes.py --batches 5
python src/replicate_to_duckdb_warehouse.py
```

> This project uses **DuckDB** for the warehouse to stay lightweight (no extra drivers).
> A Postgres version is also included in `src/replicate_to_postgres.py` (requires psycopg2).

## Outputs
- `warehouse/warehouse.duckdb` (tables: dim_user, fct_subscription, cdc_audit)
- `outputs/replication_report.json`


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
