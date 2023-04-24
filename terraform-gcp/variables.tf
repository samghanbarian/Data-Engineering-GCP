locals {
  data_lake = "de_data_lake"
}

variable "project" {
  description = "Project -id" #variables without default are manadatory
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west9"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ny_taxi_trips"
}