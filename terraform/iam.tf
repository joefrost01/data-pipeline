# IAM Configuration

resource "google_service_account" "orchestrator" {
  account_id   = "surveillance-orchestrator"
  display_name = "Surveillance Orchestrator"
  project      = var.project_id
}

# GCS permissions
resource "google_storage_bucket_iam_member" "orchestrator_landing" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "orchestrator_archive" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "orchestrator_failed" {
  bucket = google_storage_bucket.failed.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "orchestrator_staging" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

# BigQuery permissions - all datasets
resource "google_bigquery_dataset_iam_member" "orchestrator_raw" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_bigquery_dataset_iam_member" "orchestrator_staging" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_bigquery_dataset_iam_member" "orchestrator_curated" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_project_iam_member" "orchestrator_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.orchestrator.email}"
}

# Workload Identity binding
resource "google_service_account_iam_member" "orchestrator_workload_identity" {
  service_account_id = google_service_account.orchestrator.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[surveillance/orchestrator]"
}
