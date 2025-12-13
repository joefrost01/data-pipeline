

with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
),

distinct_traders as (
    select
        trader_id,
        min(event_timestamp_utc) as first_seen_utc,
        max(event_timestamp_utc) as last_seen_utc
    from events
    group by trader_id
),

final as (
    select
        -- Business key
        trader_id as trader_bk,
        
        -- Surrogate key (includes first_seen for SCD2 versioning)
        
  
    
    (hash(concat_ws('|', trader_id, first_seen_utc)) & 9223372036854775807)::bigint
  
 as trader_sk,
        
        -- Attributes
        trader_id,
        cast(null as varchar) as trader_name,  -- No reference data available
        'HUMAN' as trader_type,                 -- Default as specified
        
        -- Effective timestamp
        first_seen_utc as effective_ts
        
    from distinct_traders
)

select * from final