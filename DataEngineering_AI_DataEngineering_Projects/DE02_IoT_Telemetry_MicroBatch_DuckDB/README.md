# DE02 — IoT Telemetry Micro-Batch Pipeline (DuckDB + Parquet)

## Scenario
A fleet of devices sends telemetry events (temperature, vibration, battery, GPS). We ingest JSON files produced continuously,
process them in micro-batches every minute, and write curated Parquet datasets for analytics.

## What you’ll build
- `src/generate_stream.py` writes **JSONL** files into `data/landing/` (simulated stream)
- `src/microbatch_etl.py` processes new files **idempotently**:
  - schema validation + type casting
  - late-arrival handling (event_time vs ingest_time)
  - aggregations (per device per 5-min window)
  - writes Parquet outputs in `data/curated/`

## Tech
- Python + DuckDB
- Parquet (pyarrow)
- SQL transformations inside DuckDB
- Unit tests

## Run
```bash
python src/generate_stream.py --minutes 2
python src/microbatch_etl.py
python src/microbatch_etl.py  # safe to rerun (idempotent)
```

## Outputs
- `data/curated/events.parquet`
- `data/curated/device_5min_rollups.parquet`
- `outputs/etl_run_log.json`


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
