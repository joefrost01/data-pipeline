



with  __dbt__cte__int_instrument_current as (


with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
),

-- Get latest values for each instrument
ranked as (
    select
        exchange,
        symbol,
        contract_month,
        product_type,
        currency,
        multiplier,
        tick_size,
        event_timestamp_utc,
        row_number() over (
            partition by exchange, symbol, contract_month
            order by event_timestamp_utc desc
        ) as rn
    from events
),

latest_values as (
    select
        exchange,
        symbol,
        contract_month,
        product_type,
        currency,
        multiplier,
        tick_size
    from ranked
    where rn = 1
),

with_timestamps as (
    select
        lv.*,
        e.first_seen_utc
    from latest_values lv
    inner join (
        select
            exchange,
            symbol,
            contract_month,
            min(event_timestamp_utc) as first_seen_utc
        from events
        group by exchange, symbol, contract_month
    ) e
        on lv.exchange = e.exchange
        and lv.symbol = e.symbol
        and lv.contract_month = e.contract_month
),

final as (
    select
        -- Business key (colon-delimited as per spec)
        exchange || ':' || symbol || ':' || contract_month as instrument_bk,
        
        -- Surrogate key
        
  
    
    (hash(concat_ws('|', exchange, symbol, contract_month, first_seen_utc)) & 9223372036854775807)::bigint
  
 as instrument_sk,
        
        -- Composite identifier for convenience
        exchange || ':' || symbol || ':' || contract_month as instrument_id,
        
        -- Attributes
        symbol,
        exchange,
        product_type,
        contract_month,
        currency,
        multiplier,
        tick_size,
        
        -- Effective timestamp
        first_seen_utc as effective_ts
        
    from with_timestamps
)

select * from final
), staged as (
    select * from __dbt__cte__int_instrument_current
)






  

  

  

  

  

  

  

  



-- Incremental run: Insert new/changed records, close old versions

-- New records and updated versions
select
    source.instrument_sk,
    source.instrument_bk,
    
    source.instrument_id,
    
    source.symbol,
    
    source.exchange,
    
    source.product_type,
    
    source.contract_month,
    
    source.currency,
    
    source.multiplier,
    
    source.tick_size,
    
    source.effective_ts as valid_from_utc,
    
  
    '9999-12-31 23:59:59'::timestamp
  
 as valid_to_utc,
    true as is_current
from staged as source
left join "dev"."main_marts"."dim_instrument" as existing
    on source.instrument_bk = existing.instrument_bk
    and existing.is_current = true
where
    -- New business key (no existing current record)
    existing.instrument_bk is null
    -- Or attributes have changed
    or (coalesce(cast(source.instrument_id as varchar), '') != coalesce(cast(existing.instrument_id as varchar), '') or coalesce(cast(source.symbol as varchar), '') != coalesce(cast(existing.symbol as varchar), '') or coalesce(cast(source.exchange as varchar), '') != coalesce(cast(existing.exchange as varchar), '') or coalesce(cast(source.product_type as varchar), '') != coalesce(cast(existing.product_type as varchar), '') or coalesce(cast(source.contract_month as varchar), '') != coalesce(cast(existing.contract_month as varchar), '') or coalesce(cast(source.currency as varchar), '') != coalesce(cast(existing.currency as varchar), '') or coalesce(cast(source.multiplier as varchar), '') != coalesce(cast(existing.multiplier as varchar), '') or coalesce(cast(source.tick_size as varchar), '') != coalesce(cast(existing.tick_size as varchar), ''))

union all

-- Existing current records that need to be closed (attributes changed)
select
    existing.instrument_sk,
    existing.instrument_bk,
    
    existing.instrument_id,
    
    existing.symbol,
    
    existing.exchange,
    
    existing.product_type,
    
    existing.contract_month,
    
    existing.currency,
    
    existing.multiplier,
    
    existing.tick_size,
    
    existing.valid_from_utc,
    source.effective_ts as valid_to_utc,  -- Close at the time of change
    false as is_current
from "dev"."main_marts"."dim_instrument" as existing
inner join staged as source
    on source.instrument_bk = existing.instrument_bk
where
    existing.is_current = true
    and (coalesce(cast(source.instrument_id as varchar), '') != coalesce(cast(existing.instrument_id as varchar), '') or coalesce(cast(source.symbol as varchar), '') != coalesce(cast(existing.symbol as varchar), '') or coalesce(cast(source.exchange as varchar), '') != coalesce(cast(existing.exchange as varchar), '') or coalesce(cast(source.product_type as varchar), '') != coalesce(cast(existing.product_type as varchar), '') or coalesce(cast(source.contract_month as varchar), '') != coalesce(cast(existing.contract_month as varchar), '') or coalesce(cast(source.currency as varchar), '') != coalesce(cast(existing.currency as varchar), '') or coalesce(cast(source.multiplier as varchar), '') != coalesce(cast(existing.multiplier as varchar), '') or coalesce(cast(source.tick_size as varchar), '') != coalesce(cast(existing.tick_size as varchar), ''))


