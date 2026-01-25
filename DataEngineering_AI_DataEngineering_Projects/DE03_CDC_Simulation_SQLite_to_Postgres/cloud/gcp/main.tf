# GCP Terraform - BigQuery + GCS
provider "google" {
  project = "my-gcp-project"
  region  = "us-central1"
}

resource "google_storage_bucket" "data_bucket" {
  name          = "de-ai-${random_id.suffix.hex}"
  location      = "US"
  force_destroy = true
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = "US"
}