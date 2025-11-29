# ------------------------------------------------------------------------------
# Service Accounts
# ------------------------------------------------------------------------------

resource "google_service_account" "validator" {
  account_id   = "${local.name_prefix}-validator"
  display_name = "Surveillance Pipeline Validator"
  description  = "Service account for file validation and ingestion"
}

resource "google_service_account" "dbt" {
  account_id   = "${local.name_prefix}-dbt"
  display_name = "Surveillance Pipeline dbt"
  description  = "Service account for dbt transformations"
}

resource "google_service_account" "streaming_bridge" {
  account_id   = "${local.name_prefix}-streaming"
  display_name = "Surveillance Pipeline Streaming Bridge"
  description  = "Service account for Kafka to Pub/Sub bridge"
}

resource "google_service_account" "regulatory_reporter" {
  account_id   = "${local.name_prefix}-regulatory"
  display_name = "Surveillance Pipeline Regulatory Reporter"
  description  = "Service account for low-latency regulatory reporting"
}

resource "google_service_account" "orchestrator" {
  account_id   = "${local.name_prefix}-orchestrator"
  display_name = "Surveillance Pipeline Orchestrator"
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
  member             = "serviceAccount:${var.project_id}.svc.id.goog[surveillance/validator]"
}

resource "google_service_account_iam_member" "dbt_workload_identity" {
  service_account_id = google_service_account.dbt.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[surveillance/dbt]"
}

resource "google_service_account_iam_member" "streaming_bridge_workload_identity" {
  service_account_id = google_service_account.streaming_bridge.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[surveillance/streaming-bridge]"
}

resource "google_service_account_iam_member" "orchestrator_workload_identity" {
  service_account_id = google_service_account.orchestrator.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[surveillance/orchestrator]"
}
