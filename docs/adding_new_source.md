# Adding a New Source

Step-by-step guide to onboarding a new data source to the Markets Data Pipeline.

## Overview

Adding a source requires:

1. Source specification YAML (validation rules)
2. dbt staging model (cleaning/typing)
3. Source definition in dbt (schema)
4. Smoke test

No Python code changes required — that's the point.

---

## Step 1: Create Source Specification

Create a YAML file in `source_specs/{domain}/{source_name}.yaml`.

Use `source_specs/trading/murex_trades.yaml` as a template.

### Minimum Required Fields

```yaml
name: venue_b_trades
description: Trade executions from Venue B
owner: venue-integration@company.com
domain: markets

source:
  path_pattern: "venue_b/trades_*.csv"
  format: csv  # csv, json, jsonl, xml

schema:
  - name: exec_id
    type: STRING
    nullable: false
    
  - name: exec_time
    type: TIMESTAMP
    nullable: false
    
  - name: symbol
    type: STRING
    nullable: false
    
  - name: side
    type: STRING
    nullable: false
    allowed_values: ["BUY", "SELL"]
    
  - name: quantity
    type: INT64
    nullable: false
    min_value: 1
    
  - name: price
    type: NUMERIC
    nullable: false

expectations:
  frequency: daily
  expected_by: "07:00"
  min_files_per_day: 1

processing:
  identity:
    domain: "markets"
    source_system: "VENUE_B"
    source_id_field: "exec_id"
```

### Format-Specific Configuration

**CSV:**
```yaml
source:
  format: csv
  delimiter: ","
  has_header: true
  encoding: utf-8
```

**JSON/JSONL:**
```yaml
source:
  format: jsonl  # or json for array
  encoding: utf-8
```

**XML:**
```yaml
source:
  format: xml
  root_element: "Trades"
  row_element: "Trade"

xml_config:
  namespaces:
    ns: "http://example.com/schema"
    
schema:
  - name: trade_id
    xpath: "ns:TradeId/text()"
    type: STRING
```

### Control File (Optional)

If the source provides a control file with row counts:

```yaml
control_file:
  type: sidecar_csv       # sidecar_csv, sidecar_xml, trailer
  pattern: "{filename}.ctrl"
  row_count_field: "record_count"  # CSV column name
  required: true
```

---

## Step 2: Create dbt Staging Model

Create `dbt_project/models/staging/stg_{source_name}.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Venue B trades.
    Maps source fields to canonical schema.
*/

select
    exec_id as source_trade_id,
    'VENUE_B' as source_system,
    cast(exec_time as timestamp) as trade_time,
    cast(symbol as string) as instrument_id,
    upper(cast(side as string)) as side,
    cast(quantity as int64) as quantity,
    cast(price as numeric) as price,
    -- Fields not provided by this source
    cast(null as string) as counterparty_name,
    cast(null as string) as trader_id,
    cast(null as string) as book_id,
    current_timestamp() as loaded_at

from {{ source('raw', 'venue_b_trades') }}

where exec_id is not null
  and exec_time is not null
  and side in ('BUY', 'SELL')
  and quantity > 0
```

### Key Points

- Always cast to explicit types
- Map to canonical field names (`source_trade_id`, `trade_time`, etc.)
- Add `source_system` constant
- Filter out invalid rows (they'll be quarantined by validator anyway)
- Set missing fields to `null` with correct type
- Add `loaded_at` timestamp

---

## Step 3: Add Source Definition

Add to `dbt_project/models/staging/sources.yml`:

```yaml
sources:
  - name: raw
    tables:
      # ... existing sources ...
      
      - name: venue_b_trades
        description: Trade executions from Venue B (external table)
        external:
          location: "gs://{{ target.project }}-staging/venue_b/*.parquet"
          options:
            format: PARQUET
        columns:
          - name: exec_id
            description: Venue execution ID
          - name: exec_time
            description: Execution timestamp
          - name: symbol
            description: Instrument symbol
          - name: side
            description: BUY or SELL
          - name: quantity
            description: Executed quantity
          - name: price
            description: Execution price
```

---

## Step 4: Add Schema Tests

Add to `dbt_project/models/staging/schema.yml`:

```yaml
models:
  # ... existing models ...
  
  - name: stg_venue_b_trades
    description: Cleaned Venue B trade data
    columns:
      - name: source_trade_id
        tests:
          - not_null
      - name: trade_time
        tests:
          - not_null
      - name: side
        tests:
          - accepted_values:
              values: ['BUY', 'SELL']
      - name: quantity
        tests:
          - dbt_utils.expression_is_true:
              expression: "> 0"
```

---

## Step 5: Add to Expected Sources

Add to `dbt_project/seeds/expected_sources.csv`:

```csv
source_name,expected_by,min_files_per_day,owner
...existing sources...
venue_b_trades,07:00,1,venue-integration@company.com
```

Then run:
```bash
cd dbt_project
dbt seed
```

---

## Step 6: Add to trades_enriched (if trade data)

If this is a trade source, add to `dbt_project/models/curation/trades_enriched.sql`:

```sql
-- In the CTEs section, add:
venue_b as (
    select
        source_trade_id,
        source_system,
        trade_time,
        counterparty_name,
        trader_id,
        instrument_id,
        side,
        quantity,
        price,
        book_id,
        loaded_at
    from {{ ref('stg_venue_b_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

-- In the union, add:
all_trades as (
    select * from murex
    union all
    select * from venue_a
    union all
    select * from venue_b  -- Add this line
),
```

---

## Step 7: Smoke Test

### Option A: Using Make (recommended)

```bash
make test-source SOURCE=venue_b_trades
```

This will:
1. Drop a sample file in a dev landing bucket
2. Run one pipeline cycle
3. Assert rows appear in `surveillance_extract`

### Option B: Manual Test

1. Create a sample file matching the source spec:
   ```bash
   echo 'exec_id,exec_time,symbol,side,quantity,price
   TEST001,2025-01-15T10:30:00Z,AAPL,BUY,100,150.50
   TEST002,2025-01-15T10:31:00Z,MSFT,SELL,200,380.25' > test_trades.csv
   ```

2. Upload to landing:
   ```bash
   gsutil cp test_trades.csv gs://markets-int-landing/venue_b/trades_20250115.csv
   ```

3. Run the pipeline manually:
   ```bash
   kubectl create job --from=cronjob/surveillance-pipeline test-venue-b -n surveillance
   ```

4. Wait for completion, then verify:
   ```sql
   SELECT * FROM staging.stg_venue_b_trades
   WHERE source_trade_id LIKE 'TEST%'
   ```

5. Clean up test data:
   ```sql
   DELETE FROM curation.trades_enriched
   WHERE source_trade_id LIKE 'TEST%' AND source_system = 'VENUE_B'
   ```

---

## Checklist

Before merging PR:

- [ ] Source spec YAML created with correct schema
- [ ] Staging model maps all fields to canonical names
- [ ] Source added to `sources.yml`
- [ ] Tests added to `schema.yml`
- [ ] Added to `expected_sources.csv`
- [ ] Added to `trades_enriched.sql` (if trade data)
- [ ] Smoke test passes
- [ ] Source owner notified of expected file location and format

---

## Common Patterns

### Source Doesn't Provide trader_id

Map from another field or leave null for enrichment:

```sql
-- If source provides trader name instead of ID
left join {{ ref('traders_snapshot') }} t
    on src.trader_name = t.trader_name
    
select
    t.trader_id,  -- Looked up from name
    ...
```

### Source Has Different Field Names

Just map them in the staging model:

```sql
select
    execution_reference as source_trade_id,  -- Their name → our name
    execution_timestamp as trade_time,
    ...
```

### Source Sends Amendments

Use the deduplication config in source spec:

```yaml
processing:
  deduplication:
    key: ["trade_id"]
    strategy: "latest_version"
    version_field: "amendment_seq"
```

And handle in staging:

```sql
qualify row_number() over (
    partition by trade_id
    order by amendment_seq desc
) = 1
```

### Source is Streaming (Kafka)

1. Create source spec as normal
2. Configure Kafka bridge in `streaming/` config
3. Source lands in `raw.{source}_stream` table instead of external table
4. Staging model reads from stream table and deduplicates

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| File not picked up | Check path_pattern matches actual file location |
| Validation fails | Check schema types match actual data |
| Rows quarantined | Check nullable/allowed_values constraints |
| dbt model fails | Check source definition matches external table |
| No data in extract | Check trades_enriched union includes new source |

---

## Reference

- [design.md](design.md) — Architecture overview
- [source_specs/trading/murex_trades.yaml](../source_specs/trading/murex_trades.yaml) — Complete example
- [Support Runbook](support_runbook.md) — Operational procedures