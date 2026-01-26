#!/usr/bin/env bash
set -euo pipefail

echo "== DE04 quality demo =="
cd DE04_Data_Quality_Observability_Reports
python src/generate_daily_fact_data.py
python src/run_quality_suite.py
cd ..

echo "== AI01 vector search demo =="
cd AI01_Document_Ingestion_Vector_Index
python src/seed_documents.py
python src/build_index.py
python src/search.py --query "late arrivals buffered events" --topk 3
cd ..

echo "== AI02 churn demo =="
cd AI02_Churn_Feature_Pipeline_Training_Monitoring
python src/generate_raw_usage_data.py
python src/build_features.py
python src/train_model.py
python src/batch_score.py
python src/monitor_drift.py
cd ..

echo "== AI03 anomaly demo =="
cd AI03_Transaction_Anomaly_Scoring_Pipeline
python src/generate_transactions.py
python src/train_anomaly_model.py
python src/score_transactions.py
python src/monitor_pipeline.py
cd ..

echo "Done."
