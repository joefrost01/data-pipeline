{{
  config(
    materialized='incremental',
    unique_key='_load_id || trade_id',
    on_schema_change='append_new_columns'
  )
}}

with source as (
    select * from {{ source('raw', 'raw_trades') }}
    {% if is_incremental() %}
    where _load_id not in (select distinct _load_id from {{ this }})
    {% endif %}
),

-- Defensive casting - nullify bad data rather than failing
typed as (
    select
        trade_id,
        {{ safe_cast('trade_date', 'date') }} as trade_date,
        instrument,
        {{ safe_cast('quantity', 'numeric') }} as quantity,
        {{ safe_cast('price', 'numeric') }} as price,
        counterparty,
        _extra,
        _load_id,
        -- Flag rows with casting issues
        case
            when {{ safe_cast('quantity', 'numeric') }} is null
                 and quantity is not null
            then true
            else false
        end as _has_data_quality_issues
    from source
)

select * from typed