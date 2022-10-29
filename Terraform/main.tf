terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region
  force_destroy = true

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }
}

resource "google_storage_bucket_object" "dim_date" {
  name   = "dim_date.parquet"
  source = "../data/date.parquet"
  bucket = "${local.data_lake_bucket}_${var.project}"
}

resource "google_storage_bucket_object" "dim_time" {
  name   = "dim_time.parquet"
  source = "../data/time.parquet"
  bucket = "${local.data_lake_bucket}_${var.project}"
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  description = "San Francisco crime dataset"
  location   = var.region

  labels = {
    env = "default"
  }
}

# resource "google_bigquery_table" "table" {
#   dataset_id = google_bigquery_dataset.dataset.dataset_id
#   project = var.project
#   table_id = var.TABLE_NAME
#   deletion_protection = false

#   time_partitioning {
#     type = "DAY"
#   }

#   labels = {
#     env = "default"
#   }
# }

# ETL - Google Cloud Functions
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloudfunctions_function
locals {
  timestamp = formatdate("YYMMDDhhmmss", timestamp())
	root_dir = abspath("../")
  service_account = jsondecode(file(var.credentials)).client_email
}

# Compress source code
data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${local.root_dir}/src"
  output_path = "/tmp/function-${local.timestamp}.zip"
}

resource "google_storage_bucket" "source-bucket" {
  name     = "sf-crime-data-pipeline-source-bucket"
  location = var.region
}

# Add source code zip to bucket
resource "google_storage_bucket_object" "zip" {
  # Append file MD5 to force bucket to be recreated
  name   = "source.zip#${data.archive_file.source.output_md5}"
  bucket = google_storage_bucket.source-bucket.name
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions_function" "function" {
  name        = "SF_crime_pipeline_ELT"
  description = "SF crime data pipeline ELT function"
  runtime     = "python310"

  available_memory_mb           = 2048
  source_archive_bucket         = google_storage_bucket.source-bucket.name
  source_archive_object         = google_storage_bucket_object.zip.name
  trigger_http                  = true
  https_trigger_security_level  = "SECURE_ALWAYS"
  timeout                       = 300
  entry_point                   = "main"
}

# IAM entry for all users to invoke the function
resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = google_cloudfunctions_function.function.project
  region         = google_cloudfunctions_function.function.region
  cloud_function = google_cloudfunctions_function.function.name

  role   = "roles/cloudfunctions.invoker"
  member = "serviceAccount:${local.service_account}"
}

resource "google_cloud_scheduler_job" "job" {
  name             = "Trigger"
  description      = "Trigger ${google_cloudfunctions_function.function.name} Cloud Function everyday at 7pm."
  schedule         = "0 20 * * *" # Everyday at 8pm
  time_zone        = "America/Los_Angeles"
  attempt_deadline = "320s"

  # retry_config {
  #   retry_count = 1
  # }

  http_target {
    http_method = "GET"
    uri         = google_cloudfunctions_function.function.https_trigger_url

    oidc_token {
      service_account_email = local.service_account
    }
  }
}