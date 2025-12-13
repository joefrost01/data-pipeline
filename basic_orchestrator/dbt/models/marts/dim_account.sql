{{
  config(
    materialized='incremental',
    unique_key='account_sk',
    on_schema_change='append_new_columns'
  )
}}

{#
  Account dimension with SCD Type 2 history.
  
  Business key: account_id
  
  Minimal attributes currently - structure supports future
  reference data integration with additional account metadata.
#}

with staged as (
    select * from {{ ref('int_account_current') }}
)

{{ scd2_merge(
    source_cte='staged',
    business_key='account_bk',
    sk_column='account_sk',
    attribute_columns=['account_id'],
    effective_ts_column='effective_ts'
) }}
