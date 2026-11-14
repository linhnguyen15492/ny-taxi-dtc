terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "local" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS Bucket for Data Lake
resource "google_storage_bucket" "data_lake" {
  name          = "${var.bucket_name}-${var.environment}"
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    project     = "ny-taxi-dtc"
    managed-by  = "terraform"
  }
}

# Subfolders in the bucket
resource "google_storage_bucket_object" "raw_data" {
  name    = "raw/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "processed_data" {
  name    = "processed/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "features_data" {
  name    = "features/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}

resource "google_storage_bucket_object" "models_data" {
  name    = "models/"
  bucket  = google_storage_bucket.data_lake.name
  content = " "
}
