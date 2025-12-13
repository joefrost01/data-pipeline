{{
  config(
    materialized='view'
  )
}}

{#
  Current view of futures order events.
  
  Filters the bi-temporal fact table to show only the latest version
  of each event. Use this for day-to-day analytics to avoid double-counting.
  
  For time-travel queries (what did we know at time X?), query the
  base fact_futures_order_event table directly with:
  
    WHERE valid_from_utc <= @as_of_timestamp
      AND valid_to_utc > @as_of_timestamp
#}

select
    fact_event_sk,
    event_bk,
    business_date,
    
    -- Dimensional keys
    order_sk,
    event_seq,
    date_key,
    time_key,
    event_millis,
    event_timestamp_utc,
    org_sk,
    trader_sk,
    account_sk,
    instrument_sk,
    extra_sk,
    
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
    
    -- Lineage (latest load)
    _load_id,
    feed_name,
    valid_from_utc as loaded_at

from {{ ref('fact_futures_order_event') }}
where is_current = true
