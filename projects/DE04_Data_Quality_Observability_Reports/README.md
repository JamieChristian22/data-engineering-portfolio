# DE04 — Data Quality + Observability (Freshness, Anomalies, Reports)

## Scenario
A BI team relies on daily fact tables. This project demonstrates a **practical, engineer-friendly** framework for:
- schema checks
- null/uniqueness checks
- referential integrity checks
- freshness (SLA) checks
- anomaly checks (volume + metric thresholds)
- report generation (HTML + JSON)

## Run
```bash
python src/generate_daily_fact_data.py
python src/run_quality_suite.py
```

## Outputs
- `outputs/quality_report.json`
- `outputs/quality_report.html`


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
