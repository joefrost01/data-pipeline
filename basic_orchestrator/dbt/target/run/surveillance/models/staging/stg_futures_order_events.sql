
  
  create view "dev"."main_staging"."stg_futures_order_events__dbt_tmp" as (
    



with source as (
    select * from "dev"."main"."raw_futures_order_events"
),

load_meta as (
    select * from "dev"."main_staging"."stg_load_metadata"
),

typed as (
    select
        -- Event identification
        
  
    try_cast(event_timestamp as timestamp)
  
 as event_timestamp_utc,
        
  
    try_cast(event_seq as integer)
  
 as event_seq,
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
        
  
    try_cast(multiplier as integer)
  
 as multiplier,
        
  
    try_cast(tick_size as numeric)
  
 as tick_size,
        
        -- Order parameters
        side,
        order_type,
        time_in_force,
        
  
    try_cast(quantity as integer)
  
 as quantity,
        
  
    try_cast(limit_price as numeric)
  
 as limit_price,
        
  
    try_cast(stop_price as numeric)
  
 as stop_price,
        
        -- Market data at event time
        
  
    try_cast(best_bid as numeric)
  
 as best_bid,
        
  
    try_cast(best_ask as numeric)
  
 as best_ask,
        
  
    try_cast(mid_price as numeric)
  
 as mid_price,
        
  
    try_cast(spread as numeric)
  
 as spread,
        
        -- Fill information
        
  
    try_cast(filled_quantity as integer)
  
 as filled_quantity,
        
  
    try_cast(remaining_quantity as integer)
  
 as remaining_quantity,
        
  
    try_cast(last_fill_qty as integer)
  
 as last_fill_qty,
        
  
    try_cast(last_fill_price as numeric)
  
 as last_fill_price,
        
  
    try_cast(avg_fill_price as numeric)
  
 as avg_fill_price,
        
        -- Risk
        pre_trade_risk_check,
        nullif(risk_limit_id, '') as risk_limit_id,
        
  
    try_cast(current_position as integer)
  
 as current_position,
        
  
    try_cast(max_position_limit as integer)
  
 as max_position_limit,
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
  );
