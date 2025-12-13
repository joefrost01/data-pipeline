# Futures Order Events dbt Pipeline

Transforms raw futures order events into a star schema for surveillance analytics.

## Architecture

```
Raw (append-only)                     Staging                          Marts
┌─────────────────────┐    ┌──────────────────────────┐    ┌─────────────────────────┐
│ raw_futures_order   │───▶│ stg_futures_order_events │───▶│ dim_date, dim_time      │
│ _events             │    │ stg_load_metadata        │    │ dim_org (SCD2)          │
│ load_metadata       │    └──────────────────────────┘    │ dim_trader (SCD2)       │
└─────────────────────┘                                    │ dim_account (SCD2)      │
                                                           │ dim_instrument (SCD2)   │
                                                           │ dim_order (Type 1)      │
                                                           │ fact_futures_order_event│
                                                           │ fact_..._current (view) │
                                                           └─────────────────────────┘
```

## Bi-Temporal Design

The fact table supports "time machine" queries for regulatory investigations:

| Column | Purpose |
|--------|---------|
| `event_timestamp_utc` | When the event actually happened (business time) |
| `valid_from_utc` | When we learned about this version (system time) |
| `valid_to_utc` | When this version was superseded (9999-12-31 if current) |
| `is_current` | Convenience flag for latest version |

### Querying

**Day-to-day analytics** (latest truth only):
```sql
SELECT * FROM main_marts.fact_futures_order_event_current
```

**Time travel** (what did we know at 09:15?):
```sql
SELECT * FROM main_marts.fact_futures_order_event
WHERE valid_from_utc <= '2025-12-12 09:15:00'
  AND valid_to_utc > '2025-12-12 09:15:00'
```

**Using the helper macro**:
```sql
SELECT * FROM {{ ref('fact_futures_order_event') }}
WHERE {{ as_of('2025-12-12 09:15:00') }}
```

### Correction Flow

When a corrected file arrives for the same business date:
1. Original records get `valid_to` set to correction load time, `is_current = false`
2. New records inserted with `valid_from` = correction load time, `is_current = true`
3. Full audit trail preserved

## Usage

### Local Development (DuckDB)

```bash
cd dbt
dbt run --profiles-dir . --target dev
dbt test --profiles-dir . --target dev
```

### Production (BigQuery)

```bash
export GCP_PROJECT=your-project
export BQ_DATASET=surveillance
dbt run --target prod
```

## Models

### Staging

| Model | Description |
|-------|-------------|
| `stg_load_metadata` | Parses filenames to extract feed_name, business_date |
| `stg_futures_order_events` | Type-safe events with load context |

### Dimensions

| Model | Type | Business Key |
|-------|------|--------------|
| `dim_date` | Utility | `date_key` (YYYYMMDD) |
| `dim_time` | Utility | `time_key` (HHMMSS) |
| `dim_org` | SCD2 | `legal_entity\|division\|desk\|book` |
| `dim_trader` | SCD2 | `trader_id` |
| `dim_account` | SCD2 | `account_id` |
| `dim_instrument` | SCD2 | `exchange:symbol:contract_month` |
| `dim_order` | Type 1 | `order_id` |

### Facts

| Model | Description |
|-------|-------------|
| `fact_futures_order_event` | Bi-temporal event stream (all versions) |
| `fact_futures_order_event_current` | Current truth only (view) |

## Cross-Platform Macros

Works on both DuckDB (local) and BigQuery (prod):

- `{{ sk(['col1', 'col2']) }}` - Generate surrogate keys
- `{{ safe_cast('col', 'type') }}` - Defensive type casting
- `{{ date_key_from_ts('ts') }}` - Extract YYYYMMDD integer
- `{{ time_key_from_ts('ts') }}` - Extract HHMMSS integer
- `{{ as_of('timestamp') }}` - Time-travel filter
- `{{ end_of_time() }}` - SCD2 sentinel (9999-12-31)

## File Naming Convention

The pipeline derives business context from filenames:

| Filename | feed_name | business_date |
|----------|-----------|---------------|
| `futures_order_events_20251212.csv` | `futures_order_events` | 2025-12-12 |
| `futures_order_events_20251212_v2.csv` | `futures_order_events` | 2025-12-12 |
| `futures_order_events.csv` | `futures_order_events` | T-1 (inferred) |

Latest version is determined by `loaded_at` timestamp within each `feed_name + business_date` combination.
