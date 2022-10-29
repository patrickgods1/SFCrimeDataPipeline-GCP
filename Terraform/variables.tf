locals {
  data_lake_bucket = "sf_crime_data_lake"
}

variable "project" {
  description = "GCP Project ID"
  type = string
  default = "data-pipeline"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-west1"
  type = string
}

variable "bucket_name" {
    description = "The name of the Google Cloud Storage bucket. Must be globally unique."
    default = "default_bucket"
    type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
  type = string
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default = "default_dataset"
  type = string
}

variable "TABLE_NAME" {
    description = "BigQuery Table"
    default = "default_table"
    type = string
}

variable "credentials" {
    description = "Path to GCP project service account credentials json file"
    type = string
}