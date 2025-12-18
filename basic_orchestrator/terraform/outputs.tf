# Outputs

output "buckets" {
  value = {
    landing = google_storage_bucket.landing.name
    archive = google_storage_bucket.archive.name
    failed  = google_storage_bucket.failed.name
    staging = google_storage_bucket.staging.name
  }
}

output "datasets" {
  value = {
    raw     = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}"
    staging = "${var.project_id}.${google_bigquery_dataset.staging.dataset_id}"
    curated = "${var.project_id}.${google_bigquery_dataset.curated.dataset_id}"
  }
}

output "tables" {
  value = {
    raw_futures_order_events = google_bigquery_table.raw_futures_order_events.table_id
    order_financial_markets  = google_bigquery_table.order_financial_markets.table_id
  }
}

output "service_account" {
  value = google_service_account.orchestrator.email
}
