# 02 — Real‑Time Streaming with Kafka + Spark + Delta

## What this project demonstrates
- **Streaming ingestion:** Kafka topic receives JSON clickstream events.
- **Stream processing:** Spark Structured Streaming aggregates revenue and counts per minute.
- **Lakehouse storage:** Delta table output for downstream analytics.

## Architecture (Mermaid)
```mermaid
flowchart LR
  P[Python Producer] --> K[(Kafka Topic: clickstream)]
  K --> S[Spark Structured Streaming]
  S --> D[(Delta Lake Table)]
  D --> A[Ad-hoc query (DuckDB/SQL)]
```

## Run locally
### 1) Start services
```bash
make up
```

### 2) Create topic
```bash
make topic
```

### 3) Start streaming job (in container)
```bash
make spark-job
```

### 4) Start event producer (on your machine)
In a separate terminal (local Python 3):
```bash
pip install -r spark_app/requirements.txt
make producer
```

### 5) Verify data landed
After ~1–2 minutes:
```bash
python3 spark_app/query_results_duckdb.py
```

## What to screenshot for proof
- Producer console (events sent)
- Spark streaming job running
- Delta output folder created + DuckDB query results

## Notes
- Delta contains `_delta_log` and parquet files; the DuckDB script reads parquet data files for a simple demo.
