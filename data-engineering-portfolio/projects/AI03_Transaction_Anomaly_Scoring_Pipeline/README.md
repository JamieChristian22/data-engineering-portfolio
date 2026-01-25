# AI03 — Transaction Anomaly Scoring Pipeline (IsolationForest)

## Scenario
A payments team wants an automated pipeline to:
- ingest daily transactions
- create features
- train an anomaly model
- score and alert on risky transactions
- monitor stability + drift (volume and feature distribution)

## Run
```bash
python src/generate_transactions.py
python src/train_anomaly_model.py
python src/score_transactions.py
python src/monitor_pipeline.py
```

## Outputs
- `artifacts/anomaly_model.joblib`
- `outputs/anomaly_scores.csv`
- `outputs/pipeline_monitor_report.json`


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
