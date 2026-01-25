# AWS Terraform - Minimal but Realistic
# Provisions: S3 bucket, IAM role, Glue catalog DB, Athena workgroup

provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = "de-ai-${random_id.suffix.hex}"
  force_destroy = true
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_glue_catalog_database" "analytics" {
  name = "analytics_db"
}

resource "aws_athena_workgroup" "wg" {
  name = "analytics-wg"
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_bucket.bucket}/athena-results/"
    }
  }
}