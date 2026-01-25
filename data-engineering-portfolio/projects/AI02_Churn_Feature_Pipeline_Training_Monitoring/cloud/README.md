# Cloud Deployment Variant

This folder provides **real cloud-ready infrastructure templates** for deploying this project.

## AWS
- Terraform provisions S3 + Glue Catalog + Athena
- Designed for lakehouse-style analytics
- Run:
  ```bash
  cd cloud/aws
  terraform init
  terraform apply
  ```

## GCP
- Terraform provisions GCS + BigQuery Dataset
- Designed for analytics engineering workflows
- Run:
  ```bash
  cd cloud/gcp
  terraform init
  terraform apply
  ```

## Snowflake
- SQL file creates warehouse, database, schema, role
- Run directly in Snowflake worksheet or via SnowSQL

## How this maps to the project
| Local Component | AWS | GCP | Snowflake |
|----------------|-----|-----|------------|
| DuckDB / Postgres | Athena | BigQuery | Snowflake |
| CSV / Parquet | S3 | GCS | Stage |
| dbt models | dbt-athena | dbt-bigquery | dbt-snowflake |
| Outputs | S3 objects | BigQuery tables | Snowflake tables |