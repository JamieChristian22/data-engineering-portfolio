# AI Data Engineering — 3 Real‑World, Job‑Ready Projects (10/10)

This repo contains **three end‑to‑end AI data engineering projects** you can run locally. Each project includes:
- production-style folder structure
- repeatable CLI entrypoints
- synthetic-but-realistic data generation (so it runs out of the box)
- tests, logging, and clear documentation
- measurable outputs (databases, reports, metrics)

## Projects
1. **Project 1 — Event Stream Enrichment + Propensity Scoring**
   - Simulated clickstream events → feature engineering → train model → enrich events → load to SQLite → analytics queries

2. **Project 2 — Support Ticket Knowledge Base + Semantic Search**
   - Ingest tickets + KB docs → clean + redact PII → build TF‑IDF embeddings → semantic search CLI → evaluation metrics

3. **Project 3 — Data Reliability Monitor (Drift + Anomaly Detection)**
   - Validate datasets → detect schema breaks, null spikes, distribution drift (KS test), and metric anomalies (IsolationForest) → HTML report

## How to run
Each project folder contains its own README with step-by-step commands.

> Tip: Create a virtual environment first:
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Repo layout
- `project_1_event_enrichment/`
- `project_2_semantic_support_search/`
- `project_3_data_reliability_monitor/`

## License
MIT


## 10/10 Upgrades Included
- **Docker + docker-compose** for one-command runs
- **GitHub Actions CI** (ruff + pytest) per project
- **Structured logging** (set `LOG_FORMAT=json`)
- **Data validation helpers** (required columns, non-null, uniqueness)
- **Project 2 API key auth** (optional via `API_KEY` env var)
- **Project 2 latency benchmark** (`python -m src.benchmark`)
- **Project 1 idempotent warehouse loads** (safe re-runs)
- **Project 3 alerting stubs** (writes alerts JSON; optional Slack webhook)
