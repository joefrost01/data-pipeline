



with  __dbt__cte__int_trader_current as (


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
), staged as (
    select * from __dbt__cte__int_trader_current
)






  

  

  



-- Incremental run: Insert new/changed records, close old versions

-- New records and updated versions
select
    source.trader_sk,
    source.trader_bk,
    
    source.trader_id,
    
    source.trader_name,
    
    source.trader_type,
    
    source.effective_ts as valid_from_utc,
    
  
    '9999-12-31 23:59:59'::timestamp
  
 as valid_to_utc,
    true as is_current
from staged as source
left join "dev"."main_marts"."dim_trader" as existing
    on source.trader_bk = existing.trader_bk
    and existing.is_current = true
where
    -- New business key (no existing current record)
    existing.trader_bk is null
    -- Or attributes have changed
    or (coalesce(cast(source.trader_id as varchar), '') != coalesce(cast(existing.trader_id as varchar), '') or coalesce(cast(source.trader_name as varchar), '') != coalesce(cast(existing.trader_name as varchar), '') or coalesce(cast(source.trader_type as varchar), '') != coalesce(cast(existing.trader_type as varchar), ''))

union all

-- Existing current records that need to be closed (attributes changed)
select
    existing.trader_sk,
    existing.trader_bk,
    
    existing.trader_id,
    
    existing.trader_name,
    
    existing.trader_type,
    
    existing.valid_from_utc,
    source.effective_ts as valid_to_utc,  -- Close at the time of change
    false as is_current
from "dev"."main_marts"."dim_trader" as existing
inner join staged as source
    on source.trader_bk = existing.trader_bk
where
    existing.is_current = true
    and (coalesce(cast(source.trader_id as varchar), '') != coalesce(cast(existing.trader_id as varchar), '') or coalesce(cast(source.trader_name as varchar), '') != coalesce(cast(existing.trader_name as varchar), '') or coalesce(cast(source.trader_type as varchar), '') != coalesce(cast(existing.trader_type as varchar), ''))


