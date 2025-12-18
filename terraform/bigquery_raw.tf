# BigQuery Raw Tables
# Loaded by orchestrator - all strings for flexibility

resource "google_bigquery_table" "load_metadata" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "load_metadata"
  project             = var.project_id
  deletion_protection = true

  description = "Tracks every file load"

  schema = jsonencode([
    { name = "load_id", type = "STRING", mode = "REQUIRED" },
    { name = "filename", type = "STRING", mode = "REQUIRED" },
    { name = "table_name", type = "STRING", mode = "REQUIRED" },
    { name = "row_count", type = "INTEGER", mode = "REQUIRED" },
    { name = "started_at", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "completed_at", type = "TIMESTAMP", mode = "REQUIRED" },
  ])

  labels = { layer = "raw" }
}

resource "google_bigquery_table" "raw_futures_order_events" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "raw_futures_order_events"
  project             = var.project_id
  deletion_protection = true

  description = "Raw futures order events from CSV - one row per event"

  schema = jsonencode([
    { name = "ExtractDate", type = "STRING", mode = "NULLABLE" },
    { name = "OrderMessageID", type = "STRING", mode = "NULLABLE" },
    { name = "EventSeq", type = "STRING", mode = "NULLABLE" },
    { name = "OrderStartDate", type = "STRING", mode = "NULLABLE" },
    { name = "OrderEndDate", type = "STRING", mode = "NULLABLE" },
    { name = "TraderID", type = "STRING", mode = "NULLABLE" },
    { name = "InstrumentExchange", type = "STRING", mode = "NULLABLE" },
    { name = "InstrumentTicker", type = "STRING", mode = "NULLABLE" },
    { name = "InstrumentMonthCode", type = "STRING", mode = "NULLABLE" },
    { name = "InstrumentYearCode", type = "STRING", mode = "NULLABLE" },
    { name = "EconomicsAmount", type = "STRING", mode = "NULLABLE" },
    { name = "EconomicsContractSize", type = "STRING", mode = "NULLABLE" },
    { name = "OrderPrice", type = "STRING", mode = "NULLABLE" },
    { name = "PriceCurrency", type = "STRING", mode = "NULLABLE" },
    { name = "EventType", type = "STRING", mode = "NULLABLE" },
    { name = "EventDateTime", type = "STRING", mode = "NULLABLE" },
    { name = "EventPrice", type = "STRING", mode = "NULLABLE" },
    { name = "EventPriceType", type = "STRING", mode = "NULLABLE" },
    { name = "EventAmount", type = "STRING", mode = "NULLABLE" },
    { name = "CounterpartyType", type = "STRING", mode = "NULLABLE" },
    { name = "CounterpartyName", type = "STRING", mode = "NULLABLE" },
    { name = "CounterpartyCode", type = "STRING", mode = "NULLABLE" },
    { name = "BookName", type = "STRING", mode = "NULLABLE" },
    { name = "BusinessUnit", type = "STRING", mode = "NULLABLE" },
    { name = "Region", type = "STRING", mode = "NULLABLE" },
    { name = "_load_id", type = "STRING", mode = "NULLABLE" },
    { name = "_extra", type = "STRING", mode = "NULLABLE" },
  ])

  labels = { layer = "raw", feed = "futures-order-events" }
}
