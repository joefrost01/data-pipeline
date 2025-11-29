# ------------------------------------------------------------------------------
# Outputs
# ------------------------------------------------------------------------------

output "gke_cluster_name" {
  description = "GKE cluster name"
  value       = google_container_cluster.main.name
}

output "gke_cluster_endpoint" {
  description = "GKE cluster endpoint"
  value       = google_container_cluster.main.endpoint
  sensitive   = true
}

output "buckets" {
  description = "GCS bucket names"
  value = {
    landing  = google_storage_bucket.landing.name
    staging  = google_storage_bucket.staging.name
    archive  = google_storage_bucket.archive.name
    failed   = google_storage_bucket.failed.name
    extracts = google_storage_bucket.extracts.name
  }
}

output "bigquery_datasets" {
  description = "BigQuery dataset IDs"
  value = {
    raw       = google_bigquery_dataset.raw.dataset_id
    staging   = google_bigquery_dataset.staging.dataset_id
    curation  = google_bigquery_dataset.curation.dataset_id
    consumer  = google_bigquery_dataset.consumer.dataset_id
    snapshots = google_bigquery_dataset.snapshots.dataset_id
    control   = google_bigquery_dataset.control.dataset_id
  }
}

output "pubsub_topics" {
  description = "Pub/Sub topic names"
  value = {
    rfq_stream         = google_pubsub_topic.rfq_stream.name
    regulatory_events  = google_pubsub_topic.regulatory_events.name
    dead_letter        = google_pubsub_topic.dead_letter.name
  }
}

output "service_accounts" {
  description = "Service account emails"
  value = {
    validator           = google_service_account.validator.email
    dbt                 = google_service_account.dbt.email
    streaming_bridge    = google_service_account.streaming_bridge.email
    regulatory_reporter = google_service_account.regulatory_reporter.email
    orchestrator        = google_service_account.orchestrator.email
  }
}

output "regulatory_reporter_url" {
  description = "Cloud Run regulatory reporter URL"
  value       = google_cloud_run_service.regulatory_reporter.status[0].url
}
