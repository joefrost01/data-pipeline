# GCS Buckets

resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-surveillance-landing"
  location                    = var.bucket_location
  project                     = var.project_id
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 7 }
    action { type = "Delete" }
  }

  labels = {
    environment = var.environment
    purpose     = "landing"
  }

  depends_on = [google_project_service.services]
}

resource "google_storage_bucket" "archive" {
  name                        = "${var.project_id}-surveillance-archive"
  location                    = var.bucket_location
  project                     = var.project_id
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 30 }
    action { type = "SetStorageClass", storage_class = "NEARLINE" }
  }

  lifecycle_rule {
    condition { age = 90 }
    action { type = "SetStorageClass", storage_class = "COLDLINE" }
  }

  labels = {
    environment = var.environment
    purpose     = "archive"
  }

  depends_on = [google_project_service.services]
}

resource "google_storage_bucket" "failed" {
  name                        = "${var.project_id}-surveillance-failed"
  location                    = var.bucket_location
  project                     = var.project_id
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }

  labels = {
    environment = var.environment
    purpose     = "failed"
  }

  depends_on = [google_project_service.services]
}

resource "google_storage_bucket" "staging" {
  name                        = "${var.project_id}-surveillance-staging"
  location                    = var.bucket_location
  project                     = var.project_id
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 1 }
    action { type = "Delete" }
  }

  labels = {
    environment = var.environment
    purpose     = "staging"
  }

  depends_on = [google_project_service.services]
}
