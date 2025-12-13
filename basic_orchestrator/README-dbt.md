# Surveillance dbt Pipeline

A dbt project for transforming futures order events from raw CSV through staging to a curated star schema warehouse model.

## Architecture

```
Raw (CSV/Parquet)
   ↓
Staging (stg_*)     Light typing, renames, no business logic
   ↓
Intermediate        Ephemeral CTEs for dimension extraction
   ↓
Marts               Star schema: dim_* and fact_*
   ├─ dim_date         Date dimension (natural key YYYYMMDD)
   ├─ dim_time         Time dimension (natural key HHMMSS)
   ├─ dim_org          Organisation (SCD2)
   ├─ dim_trader       Trader (SCD2)
   ├─ dim_account      Account (SCD2)
   ├─ dim_instrument   Instrument (SCD2)
   ├─ dim_order        Order (Type 1)
   └─ fact_futures_order_event   Event grain fact table
```

## Cross-Platform Support

This project runs on both **DuckDB** (local dev) and **BigQuery** (production) using a single codebase with macro shims.

### Key Macros

| Macro | Purpose |
|-------|---------|
| `{{ sk(['field1', 'field2']) }}` | Generate hash-based surrogate key |
| `{{ safe_cast('col', 'type') }}` | Cast with NULL on failure |
| `{{ date_key_from_ts('ts') }}` | Extract YYYYMMDD integer from timestamp |
| `{{ time_key_from_ts('ts') }}` | Extract HHMMSS integer from timestamp |
| `{{ millis_from_ts('ts') }}` | Extract milliseconds component |
| `{{ current_ts() }}` | Current timestamp |
| `{{ end_of_time() }}` | SCD2 sentinel (9999-12-31) |
| `{{ scd2_merge(...) }}` | SCD Type 2 dimension merge logic |

## Local Development

```bash
# From the dbt directory
cd basic_orchestrator/dbt

# Install dbt-duckdb if needed
pip install dbt-duckdb

# Run the pipeline
dbt run --target dev

# Run tests
dbt test --target dev

# Generate docs
dbt docs generate && dbt docs serve
```

## Production (BigQuery)

```bash
# Set environment variables
export GCP_PROJECT=your-project
export BQ_DATASET=surveillance

# Run
dbt run --target prod
```

## SCD Type 2 Dimensions

The dimension tables (org, trader, account, instrument) use Slowly Changing Dimension Type 2 for full history tracking:

- **is_current = true**: The active version
- **valid_from_utc**: When this version became effective
- **valid_to_utc**: When superseded (9999-12-31 for current)

This preserves organisational/reference data **as it was** at any point in time.

## Fact Table

`fact_futures_order_event` is the primary analytical fact table:

- **Grain**: One row per order lifecycle event
- **Partitioned by**: date_key (BigQuery)
- **Clustered by**: instrument_sk, org_sk, trader_sk, event_type

Event types: NEW, SENT, ACK, PARTIAL_FILL, FILL, CANCEL, REJECTED

## Testing

Built-in dbt tests validate:
- Surrogate key uniqueness
- Not-null constraints on critical fields
- Referential integrity between fact and dimensions
- Accepted values for categorical fields

All tests are configured with `severity: warn` so the pipeline continues on data quality issues while flagging them.

## Directory Structure

```
dbt/
├── dbt_project.yml           Project configuration
├── profiles.yml              Connection profiles
├── macros/
│   ├── cross_db/             Platform-agnostic macros
│   │   ├── sk.sql            Surrogate key generation
│   │   ├── safe_cast.sql     Defensive casting
│   │   ├── date_key_from_ts.sql
│   │   ├── time_key_from_ts.sql
│   │   ├── millis_from_ts.sql
│   │   └── timestamps.sql    current_ts, end_of_time
│   └── scd2/
│       └── scd2_merge.sql    SCD2 merge logic
├── models/
│   ├── sources.yml           Raw source definitions
│   ├── staging/              Light typing layer
│   ├── intermediate/         Dimension extraction (ephemeral)
│   └── marts/                Star schema output
├── seeds/                    (empty - dimensions derived from events)
└── tests/                    Custom tests
```
