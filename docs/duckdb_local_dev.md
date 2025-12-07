# Using DuckDB for Local Development

**Fast local dbt builds, BigQuery remains route-to-live**

## 1. Why we’re doing this

Developing directly against BigQuery is:

* Slow — every `dbt run` hits GCP
* Expensive — each iteration consumes bytes billed
* Annoying — VPN/SSO/service-accounts get in the way

**DuckDB fixes this for development:**

* Sub-second local builds
* Fully offline development
* Zero BigQuery cost while iterating
* Strong SQL coverage (`QUALIFY`, window functions, CTEs)

> **BigQuery is still the only production engine.**
> DuckDB is a developer convenience — not a deployment target.

---

## 2. Team Rules (short and firm)

### Engines

* **BigQuery** = canonical, route-to-live
* **DuckDB** = local development only

### DO

* Use shim macros instead of engine-specific SQL:

  * Casting → `safe_cast`, `typed_cast`, `map_type`
  * Dates/times → `now_ts`, `today_date`, `date_sub_days`, ...
  * Hashing → `md5_hex`, `sha256_hex`, `generate_trade_id`
  * Regex → `regexp_contains`, `regexp_extract`
  * Schema opts → `table_config`
* Always test locally before pushing:
  `dbt build --target local`
* Always compile for BigQuery before merging:
  `dbt compile --target int`

### DO NOT

* Write raw BigQuery SQL like `SAFE_CAST`, `CURRENT_TIMESTAMP()`
* Write DuckDB-only types like `BIGINT` directly
* Use `shim.safe_cast` (there is no shim namespace)
* Treat DuckDB incremental logic as canonical

---

## 3. Architecture (mental model)

```
Local dev (DuckDB)                Shared envs (BigQuery)
--------------------              ------------------------
dbt build --target local          dbt build --target int/prod
         \                                 /
          \                               /
                dbt project + shim layer
```

Models call shim macros → macros output the correct SQL per engine.

---

## 4. Minimal setup

### 4.1 Install

```bash
pip install dbt-duckdb dbt-bigquery pyarrow
```

### 4.2 profiles.yml (core only)

```yaml
markets_pipeline:
  target: local

  outputs:
    local:
      type: duckdb
      path: "target/local.duckdb"
      schema: main
      threads: 4
      extensions: [parquet, json]

    int:
      type: bigquery
      method: oauth
      project: markets-int-12345
      dataset: staging
      location: europe-west2
      threads: 4
```

### 4.3 Local sources setup

```sql
{% macro setup_local_sources() %}
  {% if is_duckdb() %}
    create schema if not exists raw;

    create or replace view raw.murex_trades as
    select * from read_parquet('data/raw/murex/*.parquet');

    create or replace view raw.venue_a_trades as
    select * from read_csv_auto('data/raw/venue_a/*.csv', header=true);
  {% endif %}
{% endmacro %}
```

Run:

```bash
dbt run-operation setup_local_sources --target local
```

---

## 5. Daily workflow

### First-time on a laptop

```bash
python scripts/generate_local_data.py
dbt deps
dbt run-operation setup_local_sources --target local
```

### Usual dev loop

```bash
dbt run   --target local --select my_model
dbt test  --target local --select my_model

dbt build --target local            # before pushing
dbt compile --target int            # before merging
```

If `local.duckdb` gets huge: delete it → rebuild.

---

## 6. How to write portable models

### 6.1 Casting

Wrong:

```sql
SAFE_CAST(trade_time AS TIMESTAMP)   -- BigQuery-only
CAST(quantity AS BIGINT)             -- DuckDB-specific
```

Right:

```sql
{{ safe_cast('trade_time', 'TIMESTAMP') }}
{{ typed_cast('quantity', 'INT64') }}
```

### 6.2 Dates

Wrong:

```sql
CURRENT_TIMESTAMP()
DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
```

Right:

```sql
{{ now_ts() }}
{{ date_sub_days(today_date(), 7) }}
```

### 6.3 IDs / hashing

```sql
{{ md5_hex("concat(source_system, ':', source_trade_id)") }} as trade_id
```

Or:

```sql
{{ generate_trade_id("'markets'", 'source_system', 'source_trade_id') }}
```

### 6.4 Schema config

```sql
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
```

---

## 7. Where DuckDB helps & where it doesn’t

### Great for:

* Fast iteration
* Validating joins, filters, and transformations
* Running dbt tests locally
* Early detection of SQL mistakes
* Working offline

### Need to validate on BigQuery if you rely on:

* High-precision `BIGNUMERIC`
* Geography / `ST_*` functions
* BigQuery ML
* External tables, authorised views
* BQ-specific performance features

Incremental behaviour: **canonical on BigQuery, not DuckDB**.

---

## 8. Common issues & quick fixes

| Symptom                           | Cause                 | Fix                     |
| --------------------------------- | --------------------- | ----------------------- |
| `relation raw.xxx does not exist` | Local views missing   | Run setup macro         |
| `Unknown function SAFE_CAST`      | BigQuery SQL snuck in | Replace with shim macro |
| `shim.safe_cast not defined`      | Wrong namespace       | Remove `shim.`          |
| Type mismatch                     | Hard-coded type       | Use `map_type()`        |
| Different results locally vs BQ   | Engine semantics      | Validate in `int`       |

---

## 9. Adding new shims

```sql
{% macro some_fn(a, b) %}
  {% if is_duckdb() %}
    duckdb_fn({{ a }}, {{ b }})
  {% else %}
    some_fn({{ a }}, {{ b }})
  {% endif %}
{% endmacro %}
```

Add a tiny test under `tests/shim/` and update the reference list.

---

## 10. Final mental model

* **Write for BigQuery.**
* **Let the shim layer handle portability.**
* **Use DuckDB to get feedback in milliseconds.**

If it doesn’t run in both engines through the shims, the model isn’t production-ready.
