# Terraform (AWS-style Lakehouse Resources)

This folder contains **real Terraform** you can apply in your AWS account to provision:
- S3 bucket for lakehouse data (with versioning)
- Glue Catalog database
- Athena workgroup (query results stored in S3)

### Usage
```bash
cd infra/terraform
terraform init
terraform apply -var="lake_bucket_name=your-unique-bucket-name"
```

> Running Terraform is optional for local execution; the lakehouse pipeline runs locally with DuckDB.
