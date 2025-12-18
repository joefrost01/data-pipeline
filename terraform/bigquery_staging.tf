# BigQuery Staging Views
# Typed views over raw data

resource "google_bigquery_table" "stg_load_metadata" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_load_metadata"
  project             = var.project_id
  deletion_protection = false

  description = "Staged load metadata with derived fields"

  view {
    query = <<-SQL
      WITH source AS (
        SELECT * FROM `${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.load_metadata`
      ),
      
      parsed AS (
        SELECT
          load_id,
          filename,
          table_name,
          row_count,
          started_at,
          completed_at AS loaded_at,
          REGEXP_REPLACE(REGEXP_REPLACE(filename, r'_\d{8}.*\.csv$', ''), r'\.csv$', '') AS feed_name,
          CASE
            WHEN REGEXP_CONTAINS(filename, r'_(\d{8})')
            THEN PARSE_DATE('%Y%m%d', REGEXP_EXTRACT(filename, r'_(\d{8})'))
            ELSE DATE_SUB(DATE(completed_at), INTERVAL 1 DAY)
          END AS business_date
        FROM source
      ),
      
      with_version AS (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY feed_name, business_date ORDER BY loaded_at DESC) AS version_rank
        FROM parsed
      )
      
      SELECT
        load_id,
        filename,
        table_name,
        row_count,
        started_at,
        loaded_at,
        feed_name,
        business_date,
        version_rank = 1 AS is_latest
      FROM with_version
    SQL
    use_legacy_sql = false
  }

  labels = { layer = "staging" }

  depends_on = [google_bigquery_table.load_metadata]
}

resource "google_bigquery_table" "stg_futures_order_events" {
  dataset_id          = google_bigquery_dataset.staging.dataset_id
  table_id            = "stg_futures_order_events"
  project             = var.project_id
  deletion_protection = false

  description = "Staged futures order events with proper typing"

  view {
    query = <<-SQL
      WITH source AS (
        SELECT * FROM `${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.raw_futures_order_events`
      ),
      
      load_meta AS (
        SELECT * FROM `${var.project_id}.${google_bigquery_dataset.staging.dataset_id}.stg_load_metadata`
      )
      
      SELECT
        -- Order identification
        SAFE_CAST(src.ExtractDate AS DATE) AS extract_date,
        src.OrderMessageID AS order_message_id,
        SAFE_CAST(src.EventSeq AS INT64) AS event_seq,
        SAFE_CAST(src.OrderStartDate AS TIMESTAMP) AS order_start_date,
        SAFE_CAST(src.OrderEndDate AS TIMESTAMP) AS order_end_date,
        src.TraderID AS trader_id,
        
        -- Instrument
        src.InstrumentExchange AS instrument_exchange,
        src.InstrumentTicker AS instrument_ticker,
        src.InstrumentMonthCode AS instrument_month_code,
        src.InstrumentYearCode AS instrument_year_code,
        
        -- Economics
        SAFE_CAST(src.EconomicsAmount AS FLOAT64) AS economics_amount,
        SAFE_CAST(src.EconomicsContractSize AS INT64) AS economics_contract_size,
        SAFE_CAST(src.OrderPrice AS FLOAT64) AS order_price,
        src.PriceCurrency AS price_currency,
        
        -- Event
        src.EventType AS event_type,
        SAFE_CAST(src.EventDateTime AS TIMESTAMP) AS event_date_time,
        SAFE_CAST(src.EventPrice AS FLOAT64) AS event_price,
        src.EventPriceType AS event_price_type,
        SAFE_CAST(src.EventAmount AS FLOAT64) AS event_amount,
        
        -- Counterparty
        src.CounterpartyType AS counterparty_type,
        src.CounterpartyName AS counterparty_name,
        src.CounterpartyCode AS counterparty_code,
        
        -- Unit
        src.BookName AS book_name,
        src.BusinessUnit AS business_unit,
        src.Region AS region,
        
        -- Load tracking
        src._load_id,
        src._extra,
        lm.loaded_at,
        lm.business_date AS load_business_date,
        lm.is_latest AS is_latest_load
        
      FROM source src
      LEFT JOIN load_meta lm ON src._load_id = lm.load_id
    SQL
    use_legacy_sql = false
  }

  labels = { layer = "staging" }

  depends_on = [
    google_bigquery_table.raw_futures_order_events,
    google_bigquery_table.stg_load_metadata
  ]
}
