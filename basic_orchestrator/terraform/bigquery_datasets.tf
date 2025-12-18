# BigQuery Datasets

# Raw data - loaded by orchestrator
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "surveillance_raw"
  project       = var.project_id
  location      = var.bq_location
  friendly_name = "Surveillance Raw"
  description   = "Raw data loaded from CSV files"

  labels = {
    environment = var.environment
    layer       = "raw"
  }

  depends_on = [google_project_service.services]
}

# Staging - typed views over raw
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "surveillance_staging"
  project       = var.project_id
  location      = var.bq_location
  friendly_name = "Surveillance Staging"
  description   = "Typed views over raw data"

  labels = {
    environment = var.environment
    layer       = "staging"
  }

  depends_on = [google_project_service.services]
}

# Curated - the One Big Table
resource "google_bigquery_dataset" "curated" {
  dataset_id    = "event_interaction"
  project       = var.project_id
  location      = var.bq_location
  friendly_name = "Event Interaction"
  description   = "Curated domain models"

  labels = {
    environment = var.environment
    layer       = "curated"
  }

  depends_on = [google_project_service.services]
}
