{{
  config(
    materialized='incremental',
    unique_key='event_key',
    on_schema_change='append_new_columns',
    partition_by={
      "field": "date_key",
      "data_type": "int64"
    } if target.type == 'bigquery' else none,
    cluster_by=['instrument_sk', 'org_sk', 'trader_sk', 'event_type'] if target.type == 'bigquery' else none
  )
}}

{#
  Futures order event fact table.
  
  Grain: One row per order lifecycle event.
  
  This preserves the full event stream for forensic analysis,
  surveillance, and replay capabilities. Each event captures:
  - The event itself (type, status, timestamp)
  - Dimensional references (point-in-time via SCD2 joins)
  - Fill information (for PARTIAL_FILL and FILL events)
  - Market context (bid/ask at event time)
  - Risk information
#}

with events as (
    select * from {{ ref('stg_futures_order_events') }}
),

-- Build dimensional keys for joining
-- Note: For SCD2 dimensions, we join on business key + timestamp range
with_keys as (
    select
        e.*,
        
        -- Natural event key (order_id + event_seq should be unique)
        {{ sk(['e.order_id', 'e.event_seq']) }} as event_key,
        
        -- Date and time keys
        {{ date_key_from_ts('e.event_timestamp_utc') }} as date_key,
        {{ time_key_from_ts('e.event_timestamp_utc') }} as time_key,
        {{ millis_from_ts('e.event_timestamp_utc') }} as event_millis,
        
        -- Build business keys for dimension lookups
        'Commercial Markets' || '|' || 
        case e.desk
            when 'Commodities' then 'FICC'
            when 'Rates' then 'FICC'
            when 'Macro' then 'FICC'
            when 'Equities' then 'EQUITIES'
            else 'OTHER'
        end || '|' || e.desk || '|' || e.book as org_bk,
        
        e.trader_id as trader_bk,
        e.account_id as account_bk,
        e.exchange || ':' || e.symbol || ':' || e.contract_month as instrument_bk
        
    from events e
),

-- Join to dimensions
-- For SCD2 dimensions, we need point-in-time lookup
with_dimensions as (
    select
        k.event_key,
        k.date_key,
        k.time_key,
        k.event_millis,
        k.event_timestamp_utc,
        
        -- Order dimension (Type 1 - simple join)
        o.order_sk,
        k.event_seq,
        
        -- SCD2 dimension lookups (current version for now)
        -- In a full implementation, you'd join on valid_from <= event_ts < valid_to
        org.org_sk,
        t.trader_sk,
        a.account_sk,
        i.instrument_sk,
        
        -- Event attributes
        k.event_type,
        k.order_status,
        
        -- Fill information
        k.filled_quantity,
        k.remaining_quantity,
        k.last_fill_qty,
        k.last_fill_price,
        k.avg_fill_price,
        
        -- Market context
        k.best_bid,
        k.best_ask,
        k.mid_price,
        k.spread,
        
        -- Risk information
        k.pre_trade_risk_check,
        k.risk_limit_id,
        k.current_position,
        k.max_position_limit,
        k.reject_reason,
        
        -- Lineage
        k._load_id
        
    from with_keys k
    
    -- Order dimension
    left join {{ ref('dim_order') }} o
        on k.order_id = o.order_id
    
    -- Org dimension (SCD2 - join to current for now)
    left join {{ ref('dim_org') }} org
        on k.org_bk = org.org_bk
        and org.is_current = true
    
    -- Trader dimension (SCD2)
    left join {{ ref('dim_trader') }} t
        on k.trader_bk = t.trader_bk
        and t.is_current = true
    
    -- Account dimension (SCD2)
    left join {{ ref('dim_account') }} a
        on k.account_bk = a.account_bk
        and a.is_current = true
    
    -- Instrument dimension (SCD2)
    left join {{ ref('dim_instrument') }} i
        on k.instrument_bk = i.instrument_bk
        and i.is_current = true
)

select
    -- Keys
    event_key,
    order_sk,
    event_seq,
    
    -- Time dimensions
    date_key,
    time_key,
    event_millis,
    event_timestamp_utc,
    
    -- Dimensional foreign keys
    org_sk,
    trader_sk,
    account_sk,
    instrument_sk,
    
    -- Event
    event_type,
    order_status,
    
    -- Fills
    filled_quantity,
    remaining_quantity,
    last_fill_qty,
    last_fill_price,
    avg_fill_price,
    
    -- Market
    best_bid,
    best_ask,
    mid_price,
    spread,
    
    -- Risk
    pre_trade_risk_check,
    risk_limit_id,
    current_position,
    max_position_limit,
    reject_reason,
    
    -- Lineage
    _load_id

from with_dimensions

{% if is_incremental() %}
-- Only load events we haven't processed
where event_key not in (select event_key from {{ this }})
{% endif %}
