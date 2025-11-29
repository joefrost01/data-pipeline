# ------------------------------------------------------------------------------
# BigQuery Datasets
# ------------------------------------------------------------------------------

resource "google_bigquery_dataset" "raw" {
  dataset_id  = "raw"
  description = "External tables and streaming ingestion tables"
  location    = var.region
  labels      = local.labels

  default_table_expiration_ms = 2592000000 # 30 days

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.validator.email
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.streaming_bridge.email
  }

  access {
    role           = "READER"
    user_by_email  = google_service_account.dbt.email
  }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id  = "staging"
  description = "Clean, typed source data from dbt staging models"
  location    = var.region
  labels      = local.labels

  default_table_expiration_ms = 2592000000 # 30 days

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.dbt.email
  }
}

resource "google_bigquery_dataset" "curation" {
  dataset_id  = "curation"
  description = "Enriched, joined data with deterministic trade_id"
  location    = var.region
  labels      = local.labels

  # No expiration - 7 year retention managed by table-level config

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.dbt.email
  }
}

resource "google_bigquery_dataset" "consumer" {
  dataset_id  = "consumer"
  description = "Mart tables and views for downstream consumption"
  location    = var.region
  labels      = local.labels

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.dbt.email
  }

  # Surveillance partner read access - use the computed SA email
  access {
    role           = "READER"
    user_by_email  = local.surveillance_partner_sa
  }
}

resource "google_bigquery_dataset" "snapshots" {
  dataset_id  = "snapshots"
  description = "SCD Type 2 dimension snapshots"
  location    = var.region
  labels      = local.labels

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.dbt.email
  }

  access {
    role           = "READER"
    user_by_email  = google_service_account.regulatory_reporter.email
  }
}

resource "google_bigquery_dataset" "control" {
  dataset_id  = "control"
  description = "Pipeline control and audit tables"
  location    = var.region
  labels      = local.labels

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.validator.email
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.dbt.email
  }

  access {
    role           = "WRITER"
    user_by_email  = google_service_account.regulatory_reporter.email
  }
}

# ------------------------------------------------------------------------------
# BigQuery Tables (from schema files)
# ------------------------------------------------------------------------------

resource "google_bigquery_table" "validation_runs" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "validation_runs"
  description         = "Audit log of all file validation attempts"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/validation_runs.json")

  time_partitioning {
    type  = "DAY"
    field = "run_timestamp"
  }

  clustering = ["source_name"]
}

resource "google_bigquery_table" "dbt_runs" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "dbt_runs"
  description         = "Audit log of dbt model executions"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/dbt_runs.json")

  time_partitioning {
    type  = "DAY"
    field = "run_timestamp"
  }

  clustering = ["model_name"]
}

resource "google_bigquery_table" "source_completeness" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "source_completeness"
  description         = "Daily source arrival tracking"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/source_completeness.json")

  time_partitioning {
    type  = "DAY"
    field = "business_date"
  }

  clustering = ["source_name", "status"]
}

resource "google_bigquery_table" "regulatory_submissions" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "regulatory_submissions"
  description         = "Audit trail of regulatory report submissions"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/regulatory_submissions.json")

  time_partitioning {
    type  = "DAY"
    field = "submitted_at"
  }

  # Cluster by status and report_type for common query patterns
  # (finding failed submissions, filtering by report type)
  clustering = ["status", "report_type"]
}

resource "google_bigquery_table" "regulatory_dead_letter" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "regulatory_dead_letter"
  description         = "Failed regulatory submissions for manual review"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/regulatory_dead_letter.json")

  time_partitioning {
    type  = "DAY"
    field = "failed_at"
  }
}

resource "google_bigquery_table" "streaming_sequence_gaps" {
  dataset_id          = google_bigquery_dataset.control.dataset_id
  table_id            = "streaming_sequence_gaps"
  description         = "Detected gaps in streaming message sequences"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/streaming_sequence_gaps.json")

  time_partitioning {
    type  = "DAY"
    field = "detected_at"
  }

  clustering = ["source_system", "severity"]
}

# ------------------------------------------------------------------------------
# Streaming ingestion table (raw layer)
# ------------------------------------------------------------------------------

resource "google_bigquery_table" "rfq_stream" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "rfq_stream"
  description         = "Streaming RFQ events from Kafka via Pub/Sub"
  deletion_protection = var.enable_deletion_protection
  labels              = local.labels

  schema = file("${path.module}/schemas/rfq_stream.json")

  time_partitioning {
    type  = "DAY"
    field = "rfq_timestamp"
  }

  clustering = ["trader_id", "instrument"]
}
