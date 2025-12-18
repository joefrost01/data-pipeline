{{
    config(
        materialized='view',
        schema=var('staging_dataset', 'surveillance_staging')
    )
}}

{#
    Staged futures order events with proper typing.
    
    Transforms raw string columns to appropriate types using safe casts.
    Joins to load metadata for lineage tracking.
    
    Grain: One row per event (same as raw)
#}

with source as (
    select * from {{ source('raw', 'raw_futures_order_events') }}
),

load_meta as (
    select * from {{ ref('stg_load_metadata') }}
),

typed as (
    select
        -- Order identification
        {{ safe_cast('src.ExtractDate', 'DATE') }} as extract_date,
        src.OrderMessageID as order_message_id,
        {{ safe_cast('src.EventSeq', 'INT64') }} as event_seq,
        {{ safe_cast('src.OrderStartDate', 'TIMESTAMP') }} as order_start_date,
        {{ safe_cast('src.OrderEndDate', 'TIMESTAMP') }} as order_end_date,
        src.TraderID as trader_id,
        
        -- Instrument
        src.InstrumentExchange as instrument_exchange,
        src.InstrumentTicker as instrument_ticker,
        src.InstrumentMonthCode as instrument_month_code,
        src.InstrumentYearCode as instrument_year_code,
        
        -- Economics
        {{ safe_cast('src.EconomicsAmount', 'FLOAT64') }} as economics_amount,
        {{ safe_cast('src.EconomicsContractSize', 'INT64') }} as economics_contract_size,
        {{ safe_cast('src.OrderPrice', 'FLOAT64') }} as order_price,
        src.PriceCurrency as price_currency,
        
        -- Event
        src.EventType as event_type,
        {{ safe_cast('src.EventDateTime', 'TIMESTAMP') }} as event_date_time,
        {{ safe_cast('src.EventPrice', 'FLOAT64') }} as event_price,
        src.EventPriceType as event_price_type,
        {{ safe_cast('src.EventAmount', 'FLOAT64') }} as event_amount,
        
        -- Counterparty
        src.CounterpartyType as counterparty_type,
        src.CounterpartyName as counterparty_name,
        src.CounterpartyCode as counterparty_code,
        
        -- Unit
        src.BookName as book_name,
        src.BusinessUnit as business_unit,
        src.Region as region,
        
        -- Load tracking
        src._load_id,
        src._extra,
        lm.loaded_at,
        lm.business_date as load_business_date,
        lm.is_latest as is_latest_load
        
    from source src
    left join load_meta lm on src._load_id = lm.load_id
)

select * from typed
