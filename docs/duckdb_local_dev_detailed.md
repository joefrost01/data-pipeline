# Using DuckDB for Local Development

How to develop fast locally with DuckDB while keeping BigQuery as the route-to-live.

## Overview

Local development against BigQuery is slow. Every `dbt run` round-trips to Google, costing time and money. DuckDB gives us:

- **Sub-second feedback** — compile and run models locally in milliseconds
- **Offline development** — no network dependency, no credentials dance
- **Zero cost** — no BigQuery compute charges during development
- **Full SQL coverage** — DuckDB supports modern SQL including `QUALIFY`, window functions, CTEs

The trade-off: we need a thin compatibility layer to handle dialect differences. This document describes that layer and the workflow around it.

**Important:** BigQuery is the only route-to-live engine. DuckDB is a developer-only convenience and not a deployment target. We accept a small amount of extra macro complexity in exchange for faster iteration, offline dev, and fewer fights with GCP networking.

---

## Team Contract

This document defines the team standard for local development. Everyone on the team agrees to:

1. **Use shim macros** for any function that differs between BigQuery and DuckDB
2. **Test locally first** before pushing to `int`
3. **Add new shims** when encountering unsupported functions (and document them)
4. **Review PRs** for shim compliance — no raw `SAFE_CAST`, `CURRENT_TIMESTAMP()`, etc.

If you're not sure whether something needs a shim, compile for both targets and compare the output.

---

## ⚠️ Critical: Macro Namespacing

> **dbt macros do NOT have folder-based namespaces.**
>
> The `macros/shim/` folder is for organisation only. All macros live in a flat project namespace.
>
> **CORRECT:**
> ```sql
> {{ safe_cast('field', 'INT64') }}
> {{ now_ts() }}
> {{ md5_hex('value') }}
> ```
>
> **WRONG (will error at parse time):**
> ```sql
> {{ shim.safe_cast('field', 'INT64') }}   -- ❌ No such thing
> {{ shim.now_ts() }}                       -- ❌ Will fail
> ```
>
> If you see `shim.` anywhere in model code, it's a bug. Fix it before merging.

---

## 1. Architecture
```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Developer Workflow                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Local Machine                          CI / Shared Environments       │
│   ─────────────                          ────────────────────────       │
│                                                                         │
│   ┌─────────────┐                        ┌─────────────────────┐        │
│   │  DuckDB     │                        │     BigQuery        │        │
│   │  (local)    │                        │     (int/prod)      │        │
│   └──────┬──────┘                        └──────────┬──────────┘        │
│          │                                          │                   │
│          │  dbt build --target local                │  dbt build        │
│          │                                          │  --target int     │
│          ▼                                          ▼                   │
│   ┌──────────────────────────────────────────────────────────────┐      │
│   │                      dbt Models                              │      │
│   │                                                              │      │
│   │   staging/ → curation/ → consumer/                           │      │
│   │                                                              │      │
│   │   Uses shim macros for dialect portability                   │      │
│   └──────────────────────────────────────────────────────────────┘      │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────┐      │
│   │                    Shim Macro Layer                          │      │
│   │                                                              │      │
│   │   {{ safe_cast(...) }}                                       │      │
│   │   {{ now_ts() }}                                             │      │
│   │   {{ md5_hex(...) }}                                         │      │
│   │                                                              │      │
│   │   Translates to correct SQL per target                       │      │
│   └──────────────────────────────────────────────────────────────┘      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Project Setup

### 2.1 Install Dependencies
```bash
# Add dbt-duckdb to your dev dependencies
pip install dbt-duckdb dbt-bigquery

# Or in pyproject.toml
[project.optional-dependencies]
dev = [
    "dbt-duckdb>=1.7.0",
    "dbt-bigquery>=1.7.0",
]
```

### 2.2 Profile Configuration

Create or update `~/.dbt/profiles.yml`:
```yaml
markets_pipeline:
  target: local  # Default to local for developer convenience
  
  outputs:
    # ─────────────────────────────────────────────────────────────────
    # LOCAL: DuckDB for fast iteration
    # ─────────────────────────────────────────────────────────────────
    local:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH', 'target/local.duckdb') }}"
      threads: 4
      schema: main
      
      # Extensions we need
      extensions:
        - parquet
        - json
    
    # ─────────────────────────────────────────────────────────────────
    # INT: BigQuery integration environment
    # ─────────────────────────────────────────────────────────────────
    int:
      type: bigquery
      method: oauth  # or service-account for CI
      project: markets-int-12345
      dataset: staging
      threads: 4
      location: europe-west2
      priority: interactive
      maximum_bytes_billed: 10000000000  # 10GB safety limit
    
    # ─────────────────────────────────────────────────────────────────
    # PROD: BigQuery production (CI only, not for local use)
    # ─────────────────────────────────────────────────────────────────
    prod:
      type: bigquery
      method: service-account
      project: markets-prod-12345
      dataset: staging
      threads: 4
      location: europe-west2
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
```

### 2.3 Project Structure
```
dbt_project/
├── dbt_project.yml
├── profiles.yml              # Can also live here for team consistency
│
├── data/                     # Local test data for DuckDB
│   └── raw/
│       ├── murex/
│       │   └── *.parquet     # Can have multiple files
│       ├── venue_a/
│       │   └── *.csv
│       └── reference/
│           ├── traders.csv
│           └── counterparties.csv
│
├── macros/
│   ├── shim/                 # Dialect compatibility layer
│   │   ├── _core.sql         # is_duckdb(), is_bigquery(), map_type()
│   │   ├── casting.sql       # safe_cast(), typed_cast()
│   │   ├── dates.sql         # now_ts(), today_date(), date_sub_days()
│   │   ├── strings.sql       # safe_divide(), regexp_contains()
│   │   ├── hashing.sql       # md5_hex(), sha256_hex(), gen_uuid()
│   │   └── schema.sql        # table_config()
│   │
│   ├── generate_trade_id.sql # Uses md5_hex internally
│   ├── setup_local_sources.sql
│   └── ...
│
├── models/
│   ├── staging/
│   ├── curation/
│   └── consumer/
│
├── seeds/                    # Static reference data (works on both)
│   ├── expected_sources.csv
│   └── holiday_calendar.csv
│
└── target/                   # Generated - contains local.duckdb
    └── local.duckdb
```

---

## 3. The Shim Macro Layer

The shim layer hides dialect differences. Models call shim macros; the macros emit the right SQL for the current target.

### 3.1 Core Helpers (`macros/shim/_core.sql`)
```sql
{#
    Core shim utilities.
    
    These are the building blocks used by other shim macros.
#}

{% macro is_duckdb() %}
    {{ return(target.type == 'duckdb') }}
{% endmacro %}


{% macro is_bigquery() %}
    {{ return(target.type == 'bigquery') }}
{% endmacro %}


{#
    Map BigQuery types to DuckDB equivalents.
    Models use BigQuery type names; we translate for DuckDB.
    
    Unknown types fall back to VARCHAR for safety.
#}

{% macro map_type(bq_type) %}
    {% set type_map = {
        'INT64': 'BIGINT',
        'FLOAT64': 'DOUBLE',
        'NUMERIC': 'DECIMAL(38, 9)',
        'BIGNUMERIC': 'DECIMAL(76, 38)',
        'BOOL': 'BOOLEAN',
        'STRING': 'VARCHAR',
        'BYTES': 'BLOB',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP WITH TIME ZONE',
        'GEOGRAPHY': 'VARCHAR',
        'JSON': 'JSON',
    } %}
    
    {% set bq_type_upper = bq_type | upper %}
    
    {% if is_duckdb() %}
        {# Return mapped type, or VARCHAR as safe fallback for unknown types #}
        {{ return(type_map.get(bq_type_upper, 'VARCHAR')) }}
    {% else %}
        {{ return(bq_type) }}
    {% endif %}
{% endmacro %}
```

### 3.2 Type Casting (`macros/shim/casting.sql`)
```sql
{#
    Safe casting - returns NULL on failure instead of erroring.
    
    BigQuery: SAFE_CAST(x AS type)
    DuckDB:   TRY_CAST(x AS type)
#}

{% macro safe_cast(field, target_type) %}
    {% if is_duckdb() %}
        try_cast({{ field }} as {{ map_type(target_type) }})
    {% else %}
        safe_cast({{ field }} as {{ target_type }})
    {% endif %}
{% endmacro %}


{#
    Standard cast (errors on failure).
#}

{% macro typed_cast(field, target_type) %}
    cast({{ field }} as {{ map_type(target_type) }})
{% endmacro %}
```

### 3.3 Date and Time Functions (`macros/shim/dates.sql`)
```sql
{#
    Current timestamp.
    
    Named now_ts() to avoid collision with SQL's CURRENT_TIMESTAMP.
    
    BigQuery: CURRENT_TIMESTAMP()
    DuckDB:   current_timestamp
#}

{% macro now_ts() %}
    {% if is_duckdb() %}
        current_timestamp
    {% else %}
        current_timestamp()
    {% endif %}
{% endmacro %}


{#
    Current date.
    
    Named today_date() to avoid collision with SQL's CURRENT_DATE.
#}

{% macro today_date() %}
    {% if is_duckdb() %}
        current_date
    {% else %}
        current_date()
    {% endif %}
{% endmacro %}


{#
    Truncate timestamp to a given part.
    
    BigQuery: TIMESTAMP_TRUNC(ts, DAY)
    DuckDB:   DATE_TRUNC('day', ts)
#}

{% macro timestamp_trunc(timestamp_field, part) %}
    {% if is_duckdb() %}
        date_trunc('{{ part | lower }}', {{ timestamp_field }})
    {% else %}
        timestamp_trunc({{ timestamp_field }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Truncate date to a given part.
    
    BigQuery: DATE_TRUNC(dt, MONTH)
    DuckDB:   DATE_TRUNC('month', dt)
#}

{% macro date_trunc(date_field, part) %}
    {% if is_duckdb() %}
        date_trunc('{{ part | lower }}', {{ date_field }})
    {% else %}
        date_trunc({{ date_field }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Subtract days from a date.
    
    BigQuery: DATE_SUB(dt, INTERVAL n DAY)
    DuckDB:   dt - INTERVAL 'n days'
#}

{% macro date_sub_days(date_expr, days) %}
    {% if is_duckdb() %}
        {{ date_expr }} - interval '{{ days }} days'
    {% else %}
        date_sub({{ date_expr }}, interval {{ days }} day)
    {% endif %}
{% endmacro %}


{#
    Add days to a date.
    
    BigQuery: DATE_ADD(dt, INTERVAL n DAY)
    DuckDB:   dt + INTERVAL 'n days'
#}

{% macro date_add_days(date_expr, days) %}
    {% if is_duckdb() %}
        {{ date_expr }} + interval '{{ days }} days'
    {% else %}
        date_add({{ date_expr }}, interval {{ days }} day)
    {% endif %}
{% endmacro %}


{#
    Add interval to timestamp.
    
    BigQuery: TIMESTAMP_ADD(ts, INTERVAL 1 DAY)
    DuckDB:   ts + INTERVAL '1 day'
#}

{% macro timestamp_add(timestamp_field, interval_value, interval_unit) %}
    {% if is_duckdb() %}
        {{ timestamp_field }} + interval '{{ interval_value }} {{ interval_unit | lower }}'
    {% else %}
        timestamp_add({{ timestamp_field }}, interval {{ interval_value }} {{ interval_unit }})
    {% endif %}
{% endmacro %}


{#
    Subtract interval from timestamp.
#}

{% macro timestamp_sub(timestamp_field, interval_value, interval_unit) %}
    {% if is_duckdb() %}
        {{ timestamp_field }} - interval '{{ interval_value }} {{ interval_unit | lower }}'
    {% else %}
        timestamp_sub({{ timestamp_field }}, interval {{ interval_value }} {{ interval_unit }})
    {% endif %}
{% endmacro %}


{#
    Difference between two timestamps.
    
    BigQuery: TIMESTAMP_DIFF(ts1, ts2, SECOND)
    DuckDB:   DATEDIFF('second', ts2, ts1)  -- note argument order!
#}

{% macro timestamp_diff(timestamp1, timestamp2, part) %}
    {% if is_duckdb() %}
        datediff('{{ part | lower }}', {{ timestamp2 }}, {{ timestamp1 }})
    {% else %}
        timestamp_diff({{ timestamp1 }}, {{ timestamp2 }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Parse string to timestamp.
    
    BigQuery: PARSE_TIMESTAMP('%Y-%m-%d', str)
    DuckDB:   strptime(str, '%Y-%m-%d')
#}

{% macro parse_timestamp(format_string, string_field) %}
    {% if is_duckdb() %}
        strptime({{ string_field }}, '{{ format_string }}')
    {% else %}
        parse_timestamp('{{ format_string }}', {{ string_field }})
    {% endif %}
{% endmacro %}


{#
    Format timestamp to string.
    
    BigQuery: FORMAT_TIMESTAMP('%Y-%m-%d', ts)
    DuckDB:   strftime(ts, '%Y-%m-%d')
#}

{% macro format_timestamp(format_string, timestamp_field) %}
    {% if is_duckdb() %}
        strftime({{ timestamp_field }}, '{{ format_string }}')
    {% else %}
        format_timestamp('{{ format_string }}', {{ timestamp_field }})
    {% endif %}
{% endmacro %}
```

### 3.4 String Functions (`macros/shim/strings.sql`)
```sql
{#
    Safe divide - returns NULL instead of error on divide by zero.
    
    BigQuery: SAFE_DIVIDE(a, b)
    DuckDB:   a / NULLIF(b, 0)
#}

{% macro safe_divide(numerator, denominator) %}
    {% if is_duckdb() %}
        {{ numerator }} / nullif({{ denominator }}, 0)
    {% else %}
        safe_divide({{ numerator }}, {{ denominator }})
    {% endif %}
{% endmacro %}


{#
    Regexp contains.
    
    BigQuery: REGEXP_CONTAINS(str, r'pattern')
    DuckDB:   REGEXP_MATCHES(str, 'pattern')
#}

{% macro regexp_contains(string_field, pattern) %}
    {% if is_duckdb() %}
        regexp_matches({{ string_field }}, '{{ pattern }}')
    {% else %}
        regexp_contains({{ string_field }}, r'{{ pattern }}')
    {% endif %}
{% endmacro %}


{#
    Regexp extract.
    
    BigQuery: REGEXP_EXTRACT(str, r'pattern')
    DuckDB:   REGEXP_EXTRACT(str, 'pattern')
#}

{% macro regexp_extract(string_field, pattern) %}
    {% if is_duckdb() %}
        regexp_extract({{ string_field }}, '{{ pattern }}')
    {% else %}
        regexp_extract({{ string_field }}, r'{{ pattern }}')
    {% endif %}
{% endmacro %}
```

### 3.5 Hashing Functions (`macros/shim/hashing.sql`)
```sql
{#
    MD5 hash returning hex string.
    
    BigQuery: TO_HEX(MD5(str))
    DuckDB:   MD5(str)  -- already returns hex
#}

{% macro md5_hex(value) %}
    {% if is_duckdb() %}
        md5({{ value }})
    {% else %}
        to_hex(md5({{ value }}))
    {% endif %}
{% endmacro %}


{#
    SHA256 hash returning hex string.
    
    BigQuery: TO_HEX(SHA256(str))
    DuckDB:   SHA256(str)  -- returns hex
#}

{% macro sha256_hex(value) %}
    {% if is_duckdb() %}
        sha256({{ value }})
    {% else %}
        to_hex(sha256({{ value }}))
    {% endif %}
{% endmacro %}


{#
    Generate a random UUID.
    
    Named gen_uuid() to avoid collision with BigQuery's GENERATE_UUID().
    
    BigQuery: GENERATE_UUID()
    DuckDB:   uuid()
#}

{% macro gen_uuid() %}
    {% if is_duckdb() %}
        uuid()
    {% else %}
        generate_uuid()
    {% endif %}
{% endmacro %}
```

### 3.6 Schema and DDL (`macros/shim/schema.sql`)
```sql
{#
    Combined partitioning and clustering config for model config blocks.
    
    Partitioning and clustering are BigQuery-specific optimizations.
    On DuckDB, we just return empty config (the tables still work).
    
    Usage:
        {{ config(
            materialized='incremental',
            **table_config(
                partition_field='trade_date',
                cluster_fields=['source_system', 'instrument_id']
            )
        ) }}
#}

{% macro table_config(partition_field=none, cluster_fields=none) %}
    {% set config = {} %}
    
    {% if is_bigquery() %}
        {% if partition_field %}
            {% do config.update({'partition_by': {'field': partition_field, 'data_type': 'date'}}) %}
        {% endif %}
        {% if cluster_fields %}
            {% do config.update({'cluster_by': cluster_fields}) %}
        {% endif %}
    {% endif %}
    
    {{ return(config) }}
{% endmacro %}
```

### 3.7 Trade ID Macro

The `generate_trade_id` macro emits literal SQL — no nested string manipulation:
```sql
{#
    Generate deterministic trade ID.
    
    Emits clean SQL that works on both engines via md5_hex shim.
    
    Usage:
        {{ generate_trade_id("'markets'", 'source_system', 'source_trade_id') }}
    
    Where:
        - domain_literal is a SQL string literal (e.g., "'markets'")
        - source_system_col is a column name
        - source_trade_id_col is a column name
#}

{% macro generate_trade_id(domain_literal, source_system_col, source_trade_id_col) %}
    {{ md5_hex(
        "concat(" ~
            "'" ~ var('markets_namespace') ~ "', " ~
            "':', " ~
            domain_literal ~ ", " ~
            "':', " ~
            "cast(" ~ source_system_col ~ " as " ~ map_type('STRING') ~ "), " ~
            "':', " ~
            "cast(" ~ source_trade_id_col ~ " as " ~ map_type('STRING') ~ ")" ~
        ")"
    ) }}
{% endmacro %}
```

Usage in models:
```sql
select
    {{ generate_trade_id("'markets'", 'source_system', 'source_trade_id') }} as trade_id,
    *
from all_trades
```

---

## 4. Source Configuration

### 4.1 Source Definition

Sources use the same `raw` schema on both targets. The setup macro creates views in DuckDB that mirror BigQuery's external tables:
```yaml
# models/staging/sources.yml

version: 2

sources:
  - name: raw
    description: |
      Raw source data.
      - BigQuery: External tables over GCS, populated by ingestion pipeline
      - DuckDB: Views over local files, created by setup_local_sources macro
    schema: raw
    
    tables:
      - name: murex_trades
        description: Trade executions from Murex
        columns:
          - name: trade_id
            description: Murex trade identifier
          - name: trade_time
            description: Execution timestamp
          - name: counterparty
          - name: trader_id
          - name: instrument
          - name: side
          - name: quantity
          - name: price
          - name: book_id
          - name: notional
      
      - name: venue_a_trades
        description: Trade executions from Venue A
        columns:
          - name: exec_id
          - name: exec_time
          - name: symbol
          - name: side
          - name: qty
          - name: price
      
      - name: traders
        description: Trader reference data
        
      - name: counterparties
        description: Counterparty reference data
```

### 4.2 Loading Local Data into DuckDB

The setup macro creates the `raw` schema and views over local files. Uses wildcards so you can add files without updating the macro:
```sql
-- macros/setup_local_sources.sql

{% macro setup_local_sources() %}
    {% if is_duckdb() %}
        
        -- Create raw schema (matches BigQuery)
        create schema if not exists raw;
        
        -- Trade data (supports multiple files via wildcard)
        create or replace view raw.murex_trades as
        select * from read_parquet('data/raw/murex/*.parquet');
        
        create or replace view raw.venue_a_trades as
        select * from read_csv_auto('data/raw/venue_a/*.csv', header=true);
        
        -- Reference data
        create or replace view raw.traders as
        select * from read_csv_auto('data/raw/reference/traders.csv', header=true);
        
        create or replace view raw.counterparties as
        select * from read_csv_auto('data/raw/reference/counterparties.csv', header=true);
        
    {% else %}
        -- No-op on BigQuery
        select 1;
    {% endif %}
{% endmacro %}
```

Run this before your first build:
```bash
dbt run-operation setup_local_sources --target local
```

Or add to `dbt_project.yml` as an on-run-start hook:
```yaml
on-run-start:
  - "{{ setup_local_sources() }}"
```

### 4.3 Generating Local Test Data

Create a script to generate sample data for local development:
```python
#!/usr/bin/env python3
"""Generate sample data for local DuckDB development."""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path("dbt_project/data/raw")


def generate_murex_trades(n: int = 1000) -> None:
    """Generate sample Murex trades as Parquet."""
    
    output_dir = DATA_DIR / "murex"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    traders = [f"TRD{i:03d}" for i in range(1, 20)]
    counterparties = ["ACME Corp", "Globex", "Initech", "Umbrella", "Cyberdyne"]
    instruments = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]
    books = [f"BOOK{i:02d}" for i in range(1, 10)]
    
    records = []
    base_time = datetime.now() - timedelta(days=7)
    
    for i in range(n):
        trade_time = base_time + timedelta(
            hours=random.randint(0, 168),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        quantity = random.randint(100, 10000)
        price = round(random.uniform(50, 500), 2)
        
        records.append({
            "trade_id": f"MX-{trade_time.strftime('%Y%m%d')}-{i:06d}",
            "trade_time": trade_time.isoformat(),
            "counterparty": random.choice(counterparties),
            "trader_id": random.choice(traders),
            "instrument": random.choice(instruments),
            "side": random.choice(["BUY", "SELL"]),
            "quantity": quantity,
            "price": price,
            "book_id": random.choice(books),
            "notional": quantity * price,
        })
    
    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_dir / "trades.parquet")
    print(f"Generated {n} Murex trades → {output_dir / 'trades.parquet'}")


def generate_venue_a_trades(n: int = 500) -> None:
    """Generate sample Venue A trades as CSV."""
    
    output_dir = DATA_DIR / "venue_a"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    instruments = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    
    with open(output_dir / "trades.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "exec_id", "exec_time", "symbol", "side", "qty", "price"
        ])
        writer.writeheader()
        
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(n):
            exec_time = base_time + timedelta(
                hours=random.randint(0, 168),
                minutes=random.randint(0, 59)
            )
            
            writer.writerow({
                "exec_id": f"VA-{i:08d}",
                "exec_time": exec_time.isoformat(),
                "symbol": random.choice(instruments),
                "side": random.choice(["B", "S"]),
                "qty": random.randint(100, 5000),
                "price": round(random.uniform(100, 400), 4),
            })
    
    print(f"Generated {n} Venue A trades → {output_dir / 'trades.csv'}")


def generate_reference_data() -> None:
    """Generate reference data for dimensions."""
    
    ref_dir = DATA_DIR / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)
    
    # Traders
    with open(ref_dir / "traders.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "trader_id", "trader_name", "desk", "region", "is_active"
        ])
        writer.writeheader()
        
        desks = ["Equities", "Fixed Income", "FX", "Commodities"]
        regions = ["EMEA", "APAC", "AMER"]
        
        for i in range(1, 20):
            writer.writerow({
                "trader_id": f"TRD{i:03d}",
                "trader_name": f"Trader {i}",
                "desk": random.choice(desks),
                "region": random.choice(regions),
                "is_active": "true" if random.random() > 0.1 else "false",
            })
    
    # Counterparties
    with open(ref_dir / "counterparties.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "counterparty_id", "counterparty_name", "lei", "country"
        ])
        writer.writeheader()
        
        counterparties = [
            ("CP001", "ACME Corp", "529900T8BM49AURSDO55", "US"),
            ("CP002", "Globex", "529900HNOAA1KXQJUQ27", "GB"),
            ("CP003", "Initech", "529900LN3S50JPU47S06", "US"),
            ("CP004", "Umbrella", "529900ODI3047E2LIV03", "DE"),
            ("CP005", "Cyberdyne", "529900ZKVLT45PS35P09", "JP"),
        ]
        
        for cp_id, name, lei, country in counterparties:
            writer.writerow({
                "counterparty_id": cp_id,
                "counterparty_name": name,
                "lei": lei,
                "country": country,
            })
    
    print(f"Generated reference data → {ref_dir}")


if __name__ == "__main__":
    generate_murex_trades(1000)
    generate_venue_a_trades(500)
    generate_reference_data()
    print("\nLocal test data ready. Run: dbt build --target local")
```

---

## 5. Model Examples

### 5.1 Staging Model Using Shims
```sql
-- models/staging/stg_murex_trades.sql

{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Murex trades.
    Uses shim macros for cross-database compatibility.
*/

select
    trade_id as source_trade_id,
    'MUREX' as source_system,
    
    -- Use shim for safe casting
    {{ safe_cast('trade_time', 'TIMESTAMP') }} as trade_time,
    {{ safe_cast('quantity', 'INT64') }} as quantity,
    {{ safe_cast('price', 'NUMERIC') }} as price,
    {{ safe_cast('notional', 'NUMERIC') }} as notional,
    
    -- Standard fields (no shim needed)
    upper(side) as side,
    instrument as instrument_id,
    trader_id,
    counterparty as counterparty_name,
    book_id,
    
    -- Use shim for current timestamp
    {{ now_ts() }} as loaded_at

from {{ source('raw', 'murex_trades') }}

where trade_id is not null
  and trade_time is not null
  and side in ('BUY', 'SELL')
```

### 5.2 Curation Model with Trade ID Generation
```sql
-- models/curation/trades_enriched.sql

{{
    config(
        materialized='incremental',
        unique_key='trade_id',
        **table_config(
            partition_field='trade_date',
            cluster_fields=['source_system', 'instrument_id']
        )
    )
}}

/*
    Enriched trades from all sources.
    Generates deterministic trade_id and joins to dimensions.
*/

with murex as (
    select
        source_trade_id,
        source_system,
        trade_time,
        {{ safe_cast("date(trade_time)", 'DATE') }} as trade_date,
        instrument_id,
        side,
        quantity,
        price,
        notional,
        trader_id,
        counterparty_name,
        book_id,
        loaded_at
    from {{ ref('stg_murex_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

venue_a as (
    select
        source_trade_id,
        source_system,
        trade_time,
        {{ safe_cast("date(trade_time)", 'DATE') }} as trade_date,
        instrument_id,
        side,
        quantity,
        price,
        quantity * price as notional,
        cast(null as {{ map_type('STRING') }}) as trader_id,
        cast(null as {{ map_type('STRING') }}) as counterparty_name,
        cast(null as {{ map_type('STRING') }}) as book_id,
        loaded_at
    from {{ ref('stg_venue_a_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

all_trades as (
    select * from murex
    union all
    select * from venue_a
),

with_trade_id as (
    select
        {{ generate_trade_id("'markets'", 'source_system', 'source_trade_id') }} as trade_id,
        *
    from all_trades
)

select
    t.trade_id,
    t.source_trade_id,
    t.source_system,
    t.trade_time,
    t.trade_date,
    t.instrument_id,
    t.side,
    t.quantity,
    t.price,
    t.notional,
    t.trader_id,
    tr.trader_name,
    tr.desk,
    t.counterparty_name,
    t.book_id,
    t.loaded_at,
    
    -- Enrichment flags
    case when tr.trader_id is null and t.trader_id is not null 
         then true else false end as trader_not_found

from with_trade_id t
left join {{ ref('stg_traders') }} tr
    on t.trader_id = tr.trader_id
```

### 5.3 Consumer Model with Date Filtering
```sql
-- models/consumer/surveillance_extract.sql

{{
    config(
        materialized='table',
        **table_config(
            partition_field='trade_date',
            cluster_fields=['source_system']
        )
    )
}}

/*
    7-day rolling window for surveillance partner.
    Uses shim macros for date arithmetic.
*/

select
    trade_id,
    source_trade_id,
    source_system,
    trade_time,
    trade_date,
    instrument_id,
    side,
    quantity,
    price,
    notional,
    trader_id,
    trader_name,
    desk,
    counterparty_name,
    book_id
    
from {{ ref('trades_enriched') }}

where trade_date >= {{ date_sub_days(today_date(), 7) }}
```

---

## 6. Testing Strategy

### 6.1 Schema Tests Work on Both

Standard dbt tests work identically:
```yaml
# models/staging/schema.yml

version: 2

models:
  - name: stg_murex_trades
    columns:
      - name: source_trade_id
        tests:
          - not_null
          - unique
      - name: side
        tests:
          - accepted_values:
              values: ['BUY', 'SELL']
      - name: quantity
        tests:
          - dbt_utils.expression_is_true:
              expression: "> 0"
```

### 6.2 Custom Tests with Shims

For custom SQL tests, use shims if needed:
```sql
-- tests/assert_no_future_trades.sql

select *
from {{ ref('trades_enriched') }}
where trade_time > {{ now_ts() }}
```

### 6.3 Portability Tests

Add tests that verify shim output matches:
```sql
-- tests/shim/test_md5_deterministic.sql

with test_cases as (
    select
        {{ md5_hex("'test-input-1'") }} as hash1,
        {{ md5_hex("'test-input-1'") }} as hash2,
        {{ md5_hex("'test-input-2'") }} as hash3
)

select 'MD5 not deterministic' as failure_reason
from test_cases
where hash1 != hash2

union all

select 'MD5 collision' as failure_reason  
from test_cases
where hash1 = hash3
```

---

## 7. Developer Workflow

### 7.1 Daily Development Cycle
```bash
# 1. First time setup (once per machine)
make setup

# 2. Develop iteratively
dbt run --target local --select stg_murex_trades
dbt test --target local --select stg_murex_trades

# 3. Run full build locally before pushing
dbt build --target local

# 4. When ready, test against int (requires GCP auth)
gcloud auth application-default login
dbt build --target int --select stg_murex_trades
```

### 7.2 Makefile Shortcuts
```makefile
.PHONY: local int setup test compile docs clean

# Default target is local
local:
	cd dbt_project && dbt build --target local

# Quick local run of changed models
local-changed:
	cd dbt_project && dbt build --target local --select state:modified+

# Run against int (requires GCP credentials)
int:
	cd dbt_project && dbt build --target int

# First-time setup: generate data and create sources
setup:
	python scripts/generate_local_data.py
	cd dbt_project && dbt deps
	cd dbt_project && dbt run-operation setup_local_sources --target local

# Run all tests locally
test:
	cd dbt_project && dbt test --target local

# Compile only (fast syntax check)
compile:
	cd dbt_project && dbt compile --target local

# Generate docs
docs:
	cd dbt_project && dbt docs generate --target local
	cd dbt_project && dbt docs serve

# Clean local DuckDB and start fresh
clean:
	rm -f dbt_project/target/local.duckdb
	rm -rf dbt_project/target/
```

### 7.3 VS Code Integration

Add to `.vscode/tasks.json`:
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "dbt: build local",
            "type": "shell",
            "command": "cd dbt_project && dbt build --target local",
            "group": {
                "kind": "build",
                "isDefault": true
            },
            "problemMatcher": []
        },
        {
            "label": "dbt: run current model",
            "type": "shell",
            "command": "cd dbt_project && dbt run --target local --select ${fileBasenameNoExtension}",
            "problemMatcher": []
        },
        {
            "label": "dbt: test current model",
            "type": "shell",
            "command": "cd dbt_project && dbt test --target local --select ${fileBasenameNoExtension}",
            "problemMatcher": []
        }
    ]
}
```

---

## 8. CI/CD Integration

### 8.1 CI Pipeline
```yaml
# .github/workflows/ci.yml

name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  # Fast local checks (no GCP needed)
  local-checks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install dbt-duckdb pyarrow
          pip install -r requirements-dev.txt
      
      - name: Generate test data
        run: python scripts/generate_local_data.py
      
      - name: Setup local sources
        run: |
          cd dbt_project
          dbt deps
          dbt run-operation setup_local_sources --target local
      
      - name: Compile (syntax check)
        run: cd dbt_project && dbt compile --target local
      
      - name: Build locally
        run: cd dbt_project && dbt build --target local
      
      - name: Check namespace unchanged
        run: |
          CURRENT=$(grep 'markets_namespace' dbt_project/dbt_project.yml | cut -d"'" -f2)
          EXPECTED="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          if [ "$CURRENT" != "$EXPECTED" ]; then
            echo "::error::Namespace UUID changed!"
            exit 1
          fi

  # Integration tests against BigQuery (on main or manual trigger)
  bigquery-integration:
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' || github.event_name == 'workflow_dispatch'
    needs: local-checks
    environment: int
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: pip install dbt-bigquery
      
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      
      - name: Run against BigQuery int
        run: |
          cd dbt_project
          dbt deps
          dbt build --target int
```

### 8.2 Pre-commit Hooks
```yaml
# .pre-commit-config.yaml

repos:
  - repo: local
    hooks:
      - id: dbt-compile
        name: dbt compile check
        entry: bash -c 'cd dbt_project && dbt compile --target local'
        language: system
        files: \.sql$
        pass_filenames: false
```

---

## 9. Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| `relation "raw.xxx" does not exist` | Local sources not set up | Run `make setup` or `dbt run-operation setup_local_sources` |
| `Unknown function: SAFE_CAST` | Using BigQuery function directly | Use `{{ safe_cast(...) }}` |
| `shim.xxx is not defined` | Using non-existent namespace | Remove `shim.` prefix — just use `{{ xxx(...) }}` |
| `Type mismatch` | DuckDB/BigQuery type differences | Use `{{ map_type(...) }}` |
| `File not found: data/raw/...` | Test data not generated | Run `python scripts/generate_local_data.py` |
| `DuckDB database locked` | Multiple dbt processes | Close other terminals running dbt |

### Debugging Shims

To see what SQL a shim generates:
```bash
# Compile a specific model and inspect
dbt compile --target local --select stg_murex_trades
cat target/compiled/markets_pipeline/models/staging/stg_murex_trades.sql

# Compare with BigQuery output
dbt compile --target int --select stg_murex_trades
cat target/compiled/markets_pipeline/models/staging/stg_murex_trades.sql
```

### Adding New Shims

When you encounter a function that differs between databases:

1. Check if it's already shimmed in `macros/shim/`
2. If not, add a new macro following the pattern:
```sql
{#
    Description of what this shim does.
    
    BigQuery: SOME_FUNCTION(args)
    DuckDB:   other_function(args)
#}

{% macro some_function(arg1, arg2) %}
    {% if is_duckdb() %}
        other_function({{ arg1 }}, {{ arg2 }})
    {% else %}
        some_function({{ arg1 }}, {{ arg2 }})
    {% endif %}
{% endmacro %}
```

3. Add a test in `tests/shim/`
4. Update this guide

---

## 10. Limitations

### What Doesn't Work Locally

These BigQuery features have no DuckDB equivalent. Test in `int` if your models use them:

- BIGNUMERIC precision beyond DECIMAL(76, 38)
- Geography types and ST_* functions
- BigQuery ML
- Row-level access policies
- Streaming inserts
- External tables pointing to GCS
- Federated queries
- Authorised views

### Shim Maintenance

The shim layer is a dependency the team maintains. If discipline slips, you end up with half the code using shims and half not. PR reviews should enforce shim usage for any function in the reference table.

---

## Appendix A: Shim Reference

Quick reference for all available shim macros.

### Core

| Macro | Purpose |
|-------|---------|
| `is_duckdb()` | Returns true if target is DuckDB |
| `is_bigquery()` | Returns true if target is BigQuery |
| `map_type(bq_type)` | Convert BigQuery type to DuckDB equivalent |

### Casting

| Macro | BigQuery | DuckDB |
|-------|----------|--------|
| `safe_cast(field, type)` | `SAFE_CAST(x AS type)` | `TRY_CAST(x AS type)` |
| `typed_cast(field, type)` | `CAST(x AS type)` | `CAST(x AS mapped_type)` |

### Dates and Times

| Macro | BigQuery | DuckDB |
|-------|----------|--------|
| `now_ts()` | `CURRENT_TIMESTAMP()` | `current_timestamp` |
| `today_date()` | `CURRENT_DATE()` | `current_date` |
| `date_trunc(field, part)` | `DATE_TRUNC(field, PART)` | `date_trunc('part', field)` |
| `timestamp_trunc(field, part)` | `TIMESTAMP_TRUNC(field, PART)` | `date_trunc('part', field)` |
| `date_sub_days(expr, n)` | `DATE_SUB(expr, INTERVAL n DAY)` | `expr - interval 'n days'` |
| `date_add_days(expr, n)` | `DATE_ADD(expr, INTERVAL n DAY)` | `expr + interval 'n days'` |
| `timestamp_add(field, n, unit)` | `TIMESTAMP_ADD(...)` | `field + interval '...'` |
| `timestamp_sub(field, n, unit)` | `TIMESTAMP_SUB(...)` | `field - interval '...'` |
| `timestamp_diff(ts1, ts2, part)` | `TIMESTAMP_DIFF(ts1, ts2, PART)` | `datediff('part', ts2, ts1)` |
| `parse_timestamp(fmt, str)` | `PARSE_TIMESTAMP(fmt, str)` | `strptime(str, fmt)` |
| `format_timestamp(fmt, ts)` | `FORMAT_TIMESTAMP(fmt, ts)` | `strftime(ts, fmt)` |

### Strings and Math

| Macro | BigQuery | DuckDB |
|-------|----------|--------|
| `safe_divide(a, b)` | `SAFE_DIVIDE(a, b)` | `a / NULLIF(b, 0)` |
| `regexp_contains(str, pat)` | `REGEXP_CONTAINS(str, r'pat')` | `regexp_matches(str, 'pat')` |
| `regexp_extract(str, pat)` | `REGEXP_EXTRACT(str, r'pat')` | `regexp_extract(str, 'pat')` |

### Hashing

| Macro | BigQuery | DuckDB |
|-------|----------|--------|
| `md5_hex(value)` | `TO_HEX(MD5(value))` | `md5(value)` |
| `sha256_hex(value)` | `TO_HEX(SHA256(value))` | `sha256(value)` |
| `gen_uuid()` | `GENERATE_UUID()` | `uuid()` |

### Schema

| Macro | BigQuery | DuckDB |
|-------|----------|--------|
| `table_config(partition_field, cluster_fields)` | Returns partition/cluster config | Returns empty dict |

---

## Appendix B: Type Mappings

How `map_type()` translates BigQuery types to DuckDB:

| BigQuery | DuckDB | Notes |
|----------|--------|-------|
| `INT64` | `BIGINT` | |
| `FLOAT64` | `DOUBLE` | |
| `NUMERIC` | `DECIMAL(38, 9)` | |
| `BIGNUMERIC` | `DECIMAL(76, 38)` | Precision may differ |
| `BOOL` | `BOOLEAN` | |
| `STRING` | `VARCHAR` | |
| `BYTES` | `BLOB` | |
| `DATE` | `DATE` | |
| `TIME` | `TIME` | |
| `DATETIME` | `TIMESTAMP` | No timezone |
| `TIMESTAMP` | `TIMESTAMP WITH TIME ZONE` | |
| `GEOGRAPHY` | `VARCHAR` | No spatial support |
| `JSON` | `JSON` | |
| *(unknown)* | `VARCHAR` | Safe fallback |

---

## Summary

DuckDB local development gives us:

- **10x faster iteration** — sub-second builds vs multi-second BigQuery round-trips
- **Offline capability** — develop anywhere without network access
- **Zero cost** — no BigQuery charges during development
- **Same SQL** — models work identically via shim layer

The shim layer is ~200 lines of macros and covers 95% of use cases. For the remaining 5%, test in `int` before merging.

**Quick start:**
```bash
pip install dbt-duckdb pyarrow
make setup
make local
```