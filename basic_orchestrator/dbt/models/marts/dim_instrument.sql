{{
  config(
    materialized='incremental',
    unique_key='instrument_sk',
    on_schema_change='append_new_columns'
  )
}}

{#
  Instrument dimension with SCD Type 2 history.
  
  Business key: exchange:symbol:contract_month
  
  Tracks instrument reference data changes.
  Attributes like tick_size or multiplier can change over time
  (e.g., exchange rule changes), so SCD2 preserves the values
  as they were at the time of each event.
#}

with staged as (
    select * from {{ ref('int_instrument_current') }}
)

{{ scd2_merge(
    source_cte='staged',
    business_key='instrument_bk',
    sk_column='instrument_sk',
    attribute_columns=['instrument_id', 'symbol', 'exchange', 'product_type', 'contract_month', 'currency', 'multiplier', 'tick_size'],
    effective_ts_column='effective_ts'
) }}
