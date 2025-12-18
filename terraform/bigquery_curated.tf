# BigQuery Curated Layer
# One Big Table with nested structs

resource "google_bigquery_table" "order_financial_markets" {
  dataset_id          = google_bigquery_dataset.curated.dataset_id
  table_id            = "order_financial_markets"
  project             = var.project_id
  deletion_protection = true

  description = <<-DESC
    Curated futures order data with nested structs.
    
    Grain: One row per order (OrderMessageID) per version.
    Events are nested as a repeated struct.
    Bi-temporal at order level.
  DESC

  schema = jsonencode([
    {
      name        = "Type"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Record type: Master.Transaction"
    },
    {
      name        = "ExtractDate"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Date the data was extracted"
    },
    {
      name        = "Execution"
      type        = "RECORD"
      mode        = "REQUIRED"
      description = "Order execution details"
      fields = [
        { name = "Type", type = "STRING", mode = "REQUIRED", description = "Execution.OrderStream" },
        { name = "OrderMessageID", type = "STRING", mode = "REQUIRED", description = "Unique order identifier" },
        { name = "OrderStartDate", type = "TIMESTAMP", mode = "NULLABLE", description = "Order creation time" },
        { name = "OrderEndDate", type = "TIMESTAMP", mode = "NULLABLE", description = "Order completion time" },
        { name = "TraderID", type = "STRING", mode = "NULLABLE", description = "Encrypted trader identifier" },
        {
          name   = "Economics"
          type   = "RECORD"
          mode   = "REQUIRED"
          fields = [
            { name = "Type", type = "STRING", mode = "REQUIRED", description = "IR.RateFuture" },
            { name = "AssetType", type = "STRING", mode = "REQUIRED", description = "Interest Rate Futures" },
            { name = "Amount", type = "FLOAT64", mode = "NULLABLE", description = "Order amount/quantity" },
            { name = "ContractSize", type = "INT64", mode = "NULLABLE", description = "Contract size multiplier" },
            { name = "Price", type = "FLOAT64", mode = "NULLABLE", description = "Order price" },
            {
              name   = "PriceCurrency"
              type   = "RECORD"
              mode   = "REQUIRED"
              fields = [
                { name = "Type", type = "STRING", mode = "REQUIRED", description = "ID.ISO" },
                { name = "ISOCode", type = "STRING", mode = "NULLABLE", description = "Currency ISO code" }
              ]
            }
          ]
        },
        {
          name   = "Identifier"
          type   = "RECORD"
          mode   = "REQUIRED"
          fields = [
            { name = "Type", type = "STRING", mode = "REQUIRED", description = "ID.FutureExchange" },
            { name = "Exchange", type = "STRING", mode = "NULLABLE", description = "Exchange code" },
            { name = "Ticker", type = "STRING", mode = "NULLABLE", description = "Instrument ticker" },
            { name = "MonthCode", type = "STRING", mode = "NULLABLE", description = "Contract month code" },
            { name = "YearCode", type = "STRING", mode = "NULLABLE", description = "Contract year" }
          ]
        },
        {
          name        = "Events"
          type        = "RECORD"
          mode        = "REPEATED"
          description = "Order lifecycle events"
          fields = [
            { name = "Type", type = "STRING", mode = "REQUIRED", description = "Event type e.g. OrderEvent.Fill" },
            { name = "EventID", type = "STRING", mode = "REQUIRED", description = "Unique event identifier" },
            { name = "EventDateTime", type = "TIMESTAMP", mode = "REQUIRED", description = "Event timestamp" },
            { name = "Price", type = "FLOAT64", mode = "NULLABLE", description = "Event price" },
            { name = "PriceType", type = "STRING", mode = "NULLABLE", description = "Price type" },
            { name = "Amount", type = "FLOAT64", mode = "NULLABLE", description = "Event amount" },
            {
              name   = "Counterparty"
              type   = "RECORD"
              mode   = "NULLABLE"
              fields = [
                { name = "Type", type = "STRING", mode = "NULLABLE", description = "Counterparty type" },
                { name = "CounterpartyName", type = "STRING", mode = "NULLABLE", description = "Encrypted name" },
                { name = "CounterpartyCode", type = "STRING", mode = "NULLABLE", description = "Encrypted code" }
              ]
            }
          ]
        }
      ]
    },
    {
      name   = "Unit"
      type   = "RECORD"
      mode   = "REQUIRED"
      fields = [
        { name = "Type", type = "STRING", mode = "REQUIRED", description = "Unit.Book" },
        { name = "BookName", type = "STRING", mode = "NULLABLE", description = "Book name" },
        { name = "BusinessUnit", type = "STRING", mode = "NULLABLE", description = "Business unit" },
        { name = "Region", type = "STRING", mode = "NULLABLE", description = "Region code" }
      ]
    },
    {
      name        = "valid_from_utc"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Bi-temporal: when this version became truth"
    },
    {
      name        = "valid_to_utc"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Bi-temporal: when superseded (9999-12-31 if current)"
    },
    {
      name        = "is_current"
      type        = "BOOL"
      mode        = "REQUIRED"
      description = "True for latest version"
    },
    {
      name        = "_load_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Load identifier for lineage"
    }
  ])

  # Partition by extract date for efficient queries
  time_partitioning {
    type  = "DAY"
    field = "ExtractDate"
  }

  # Cluster for common query patterns
  clustering = ["Execution.Identifier.Exchange", "Unit.BusinessUnit", "is_current"]

  labels = {
    layer   = "curated"
    domain  = "order"
    product = "financial-markets"
  }
}

# Current view - filters to is_current = true
resource "google_bigquery_table" "order_financial_markets_current" {
  dataset_id          = google_bigquery_dataset.curated.dataset_id
  table_id            = "order_financial_markets_current"
  project             = var.project_id
  deletion_protection = false

  description = "Current view of orders - filters to is_current = true"

  view {
    query = <<-SQL
      SELECT * EXCEPT(valid_from_utc, valid_to_utc, is_current)
      FROM `${var.project_id}.${google_bigquery_dataset.curated.dataset_id}.order_financial_markets`
      WHERE is_current = TRUE
    SQL
    use_legacy_sql = false
  }

  labels = {
    layer     = "curated"
    view_type = "current"
  }

  depends_on = [google_bigquery_table.order_financial_markets]
}
