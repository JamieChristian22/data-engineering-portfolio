# AI02 — Churn Feature Pipeline + Training + Monitoring

## Scenario
A subscription SaaS wants a reliable **data engineering pipeline** around ML:
- build clean training data
- produce consistent features
- train a baseline model
- batch score daily
- monitor performance + drift

## Run
```bash
python src/generate_raw_usage_data.py
python src/build_features.py
python src/train_model.py
python src/batch_score.py
python src/monitor_drift.py
```

## Outputs
- `artifacts/model.joblib`
- `outputs/batch_predictions.csv`
- `outputs/drift_report.json`


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
