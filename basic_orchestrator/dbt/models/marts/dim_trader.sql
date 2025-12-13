{{
  config(
    materialized='incremental',
    unique_key='trader_sk',
    on_schema_change='append_new_columns'
  )
}}

{#
  Trader dimension with SCD Type 2 history.
  
  Business key: trader_id
  
  Tracks trader attribute changes over time.
  Currently attributes are minimal (name null, type HUMAN by default)
  but the structure supports future reference data integration.
#}

with staged as (
    select * from {{ ref('int_trader_current') }}
)

{{ scd2_merge(
    source_cte='staged',
    business_key='trader_bk',
    sk_column='trader_sk',
    attribute_columns=['trader_id', 'trader_name', 'trader_type'],
    effective_ts_column='effective_ts'
) }}
