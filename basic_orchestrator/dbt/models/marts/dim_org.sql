{{
  config(
    materialized='incremental',
    unique_key='org_sk',
    on_schema_change='append_new_columns'
  )
}}

{#
  Organisation dimension with SCD Type 2 history.
  
  Business key: legal_entity|division|desk|book
  
  Tracks changes to organisational attributes over time.
  When attributes change for a business key:
  1. The current record is closed (valid_to set, is_current=false)
  2. A new record is inserted with the new attributes
  
  This preserves the org structure AS IT WAS at the time of each event.
#}

with staged as (
    select * from {{ ref('int_org_current') }}
)

{{ scd2_merge(
    source_cte='staged',
    business_key='org_bk',
    sk_column='org_sk',
    attribute_columns=['legal_entity', 'division', 'desk', 'book'],
    effective_ts_column='effective_ts'
) }}
