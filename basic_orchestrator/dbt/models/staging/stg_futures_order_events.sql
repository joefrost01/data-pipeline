{{
  config(
    materialized='view'
  )
}}

{#
  Staging layer for futures order events.
  
  Responsibilities:
  - Parse timestamps from ISO strings
  - Cast numeric fields with defensive safe_cast
  - Rename columns for clarity
  - Join to load metadata for bi-temporal context
  - No business logic - that belongs in intermediate/marts
  
  All original columns preserved, just typed appropriately.
#}

with source as (
    select * from {{ source('raw', 'raw_futures_order_events') }}
),

load_meta as (
    select * from {{ ref('stg_load_metadata') }}
),

typed as (
    select
        -- Event identification
        {{ safe_cast('event_timestamp', 'timestamp') }} as event_timestamp_utc,
        {{ safe_cast('event_seq', 'integer') }} as event_seq,
        event_type,
        order_status,
        
        -- Order identifiers
        order_id,
        nullif(parent_order_id, '') as parent_order_id,
        client_order_id,
        broker_order_id,
        exchange_order_id,
        
        -- Organisation
        account_id,
        trader_id,
        desk,
        book,
        
        -- Strategy / source
        strategy_id,
        source_system,
        
        -- Instrument
        symbol,
        exchange,
        product_type,
        contract_month,
        currency,
        {{ safe_cast('multiplier', 'integer') }} as multiplier,
        {{ safe_cast('tick_size', 'numeric') }} as tick_size,
        
        -- Order parameters
        side,
        order_type,
        time_in_force,
        {{ safe_cast('quantity', 'integer') }} as quantity,
        {{ safe_cast('limit_price', 'numeric') }} as limit_price,
        {{ safe_cast('stop_price', 'numeric') }} as stop_price,
        
        -- Market data at event time
        {{ safe_cast('best_bid', 'numeric') }} as best_bid,
        {{ safe_cast('best_ask', 'numeric') }} as best_ask,
        {{ safe_cast('mid_price', 'numeric') }} as mid_price,
        {{ safe_cast('spread', 'numeric') }} as spread,
        
        -- Fill information
        {{ safe_cast('filled_quantity', 'integer') }} as filled_quantity,
        {{ safe_cast('remaining_quantity', 'integer') }} as remaining_quantity,
        {{ safe_cast('last_fill_qty', 'integer') }} as last_fill_qty,
        {{ safe_cast('last_fill_price', 'numeric') }} as last_fill_price,
        {{ safe_cast('avg_fill_price', 'numeric') }} as avg_fill_price,
        
        -- Risk
        pre_trade_risk_check,
        nullif(risk_limit_id, '') as risk_limit_id,
        {{ safe_cast('current_position', 'integer') }} as current_position,
        {{ safe_cast('max_position_limit', 'integer') }} as max_position_limit,
        nullif(reject_reason, '') as reject_reason,
        
        -- Load tracking
        src._load_id,
        src._extra,
        
        -- Bi-temporal context from load metadata
        lm.loaded_at,
        lm.feed_name,
        lm.business_date,
        lm.is_latest_for_business_date
        
    from source src
    left join load_meta lm on src._load_id = lm.load_id
)

select * from typed
