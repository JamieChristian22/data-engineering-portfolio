terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 lake bucket (bronze/silver/gold prefixes) ---
resource "aws_s3_bucket" "lake" {
  bucket = var.lake_bucket_name
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- Glue catalog database ---
resource "aws_glue_catalog_database" "lake_db" {
  name = var.glue_database_name
}

# --- Example: Athena workgroup for analytics ---
resource "aws_athena_workgroup" "wg" {
  name = var.athena_workgroup_name
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}
