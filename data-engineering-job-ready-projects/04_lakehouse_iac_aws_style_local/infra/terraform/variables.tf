variable "aws_region" {
  type        = string
  description = "AWS region"
  default     = "us-east-1"
}

variable "lake_bucket_name" {
  type        = string
  description = "Globally-unique S3 bucket name for the lake"
}

variable "glue_database_name" {
  type        = string
  description = "Glue Catalog database name"
  default     = "lakehouse_db"
}

variable "athena_workgroup_name" {
  type        = string
  description = "Athena workgroup name"
  default     = "lakehouse-analytics"
}
