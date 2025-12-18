# ------------------------------------------------------------------------------
# Service Accounts
# ------------------------------------------------------------------------------

resource "google_service_account" "validator" {
  account_id   = "${local.name_prefix}-validator"
  display_name = "Markets Pipeline Validator"
  description  = "Service account for file validation and ingestion"
}

resource "google_service_account" "dbt" {
  account_id   = "${local.name_prefix}-dbt"
  display_name = "Markets Pipeline dbt"
  description  = "Service account for dbt transformations"
}

resource "google_service_account" "streaming_bridge" {
  account_id   = "${local.name_prefix}-streaming"
  display_name = "Markets Pipeline Streaming Bridge"
  description  = "Service account for Kafka to Pub/Sub bridge"
}

resource "google_service_account" "regulatory_reporter" {
  account_id   = "${local.name_prefix}-regulatory"
  display_name = "Markets Pipeline Regulatory Reporter"
  description  = "Service account for low-latency regulatory reporting"
}

resource "google_service_account" "orchestrator" {
  account_id   = "${local.name_prefix}-orchestrator"
  display_name = "Markets Pipeline Orchestrator"
  description  = "Service account for pipeline orchestration"
}

resource "google_service_account" "pubsub_invoker" {
  account_id   = "${local.name_prefix}-pubsub-invoker"
  display_name = "Pub/Sub Cloud Run Invoker"
  description  = "Service account for Pub/Sub to invoke Cloud Run"
}

# ------------------------------------------------------------------------------
# GCS Permissions
# ------------------------------------------------------------------------------

# Validator: read landing, write staging/failed
resource "google_storage_bucket_iam_member" "validator_landing_read" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.validator.email}"
}

resource "google_storage_bucket_iam_member" "validator_landing_delete" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.validator.email}"
}

resource "google_storage_bucket_iam_member" "validator_staging_write" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.validator.email}"
}

resource "google_storage_bucket_iam_member" "validator_failed_write" {
  bucket = google_storage_bucket.failed.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.validator.email}"
}

# Orchestrator: read staging, write archive/extracts
resource "google_storage_bucket_iam_member" "orchestrator_staging_admin" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "orchestrator_archive_write" {
  bucket = google_storage_bucket.archive.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_storage_bucket_iam_member" "orchestrator_extracts_write" {
  bucket = google_storage_bucket.extracts.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.orchestrator.email}"
}

# dbt: read staging files for external tables
resource "google_storage_bucket_iam_member" "dbt_staging_read" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.dbt.email}"
}

# ------------------------------------------------------------------------------
# BigQuery Permissions
# ------------------------------------------------------------------------------

# Orchestrator needs BigQuery access for:
# - Writing to control tables (validation_runs, etc.)
# - Running extract queries

resource "google_project_iam_member" "orchestrator_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_bigquery_dataset_iam_member" "orchestrator_control_writer" {
  dataset_id = google_bigquery_dataset.control.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.orchestrator.email}"
}

resource "google_bigquery_dataset_iam_member" "orchestrator_consumer_reader" {
  dataset_id = google_bigquery_dataset.consumer.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.orchestrator.email}"
}

# Validator needs BigQuery access for writing validation_runs
resource "google_project_iam_member" "validator_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.validator.email}"
}

resource "google_bigquery_dataset_iam_member" "validator_control_writer" {
  dataset_id = google_bigquery_dataset.control.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.validator.email}"
}

# dbt needs BigQuery access for transformations
resource "google_project_iam_member" "dbt_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

resource "google_project_iam_member" "dbt_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dbt.email}"
}

# Regulatory reporter needs BigQuery access for cache refresh and audit logging
resource "google_project_iam_member" "regulatory_reporter_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.regulatory_reporter.email}"
}

resource "google_bigquery_dataset_iam_member" "regulatory_reporter_snapshots_reader" {
  dataset_id = google_bigquery_dataset.snapshots.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.regulatory_reporter.email}"
}

resource "google_bigquery_dataset_iam_member" "regulatory_reporter_curation_reader" {
  dataset_id = google_bigquery_dataset.curation.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.regulatory_reporter.email}"
}

resource "google_bigquery_dataset_iam_member" "regulatory_reporter_control_writer" {
  dataset_id = google_bigquery_dataset.control.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.regulatory_reporter.email}"
}

# Streaming bridge needs BigQuery write access for streaming inserts
resource "google_bigquery_dataset_iam_member" "streaming_bridge_raw_writer" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.streaming_bridge.email}"
}

# ------------------------------------------------------------------------------
# Pub/Sub Permissions
# ------------------------------------------------------------------------------

resource "google_pubsub_topic_iam_member" "streaming_bridge_publish" {
  topic  = google_pubsub_topic.rfq_stream.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.streaming_bridge.email}"
}

resource "google_pubsub_topic_iam_member" "streaming_bridge_regulatory_publish" {
  topic  = google_pubsub_topic.regulatory_events.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.streaming_bridge.email}"
}

# Pub/Sub invoker for Cloud Run
resource "google_cloud_run_service_iam_member" "pubsub_invoker" {
  service  = google_cloud_run_service.regulatory_reporter.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pubsub_invoker.email}"
}

# ------------------------------------------------------------------------------
# Secret Manager Access
# ------------------------------------------------------------------------------

resource "google_secret_manager_secret_iam_member" "kafka_creds_streaming" {
  secret_id = google_secret_manager_secret.kafka_credentials.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.streaming_bridge.email}"
}

resource "google_secret_manager_secret_iam_member" "regulator_api_key" {
  secret_id = google_secret_manager_secret.regulator_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.regulatory_reporter.email}"
}

# ------------------------------------------------------------------------------
# Workload Identity (GKE pods use service accounts)
# ------------------------------------------------------------------------------

resource "google_service_account_iam_member" "validator_workload_identity" {
  service_account_id = google_service_account.validator.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[markets/validator]"
}

resource "google_service_account_iam_member" "dbt_workload_identity" {
  service_account_id = google_service_account.dbt.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[markets/dbt]"
}

resource "google_service_account_iam_member" "streaming_bridge_workload_identity" {
  service_account_id = google_service_account.streaming_bridge.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[markets/streaming-bridge]"
}

resource "google_service_account_iam_member" "orchestrator_workload_identity" {
  service_account_id = google_service_account.orchestrator.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[markets/orchestrator]"
}
