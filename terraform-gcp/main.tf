terraform {
    backend "local" {} # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  //credentials = file("<NAME>.json"/var.credentials) #if it is not set by GOOGLE_APPLICATION_CREDENTIALS

  project = var.project
  region  = var.region
  //zone    = "var.zone"
}

#https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data_lake" {
  name          = "${local.data_lake}_${var.project}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
  storage_class = var.storage_class

  lifecycle_rule {
    condition {
      age = 30 #days
    }
    action {
      type = "Delete"
    }
  }

  versioning{
    enabled = true
  }
 }
#https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.BQ
  friendly_name               = "ny_taxi"
  project                     = var.project
  description                 = "This is a test description"
  location                    = var.region
  
}
