{#
  Extract distinct traders from events.
  
  Business key: trader_id
  
  Since we don't have a reference feed:
  - trader_name is left blank
  - trader_type defaults to 'HUMAN' as specified
#}

with events as (
    select * from {{ ref('stg_futures_order_events') }}
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
        {{ sk(["trader_id", "first_seen_utc"]) }} as trader_sk,
        
        -- Attributes
        trader_id,
        cast(null as varchar) as trader_name,  -- No reference data available
        'HUMAN' as trader_type,                 -- Default as specified
        
        -- Effective timestamp
        first_seen_utc as effective_ts
        
    from distinct_traders
)

select * from final
