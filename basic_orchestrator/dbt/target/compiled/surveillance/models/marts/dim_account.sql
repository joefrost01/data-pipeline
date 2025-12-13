



with  __dbt__cte__int_account_current as (


with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
),

distinct_accounts as (
    select
        account_id,
        min(event_timestamp_utc) as first_seen_utc,
        max(event_timestamp_utc) as last_seen_utc
    from events
    group by account_id
),

final as (
    select
        -- Business key
        account_id as account_bk,
        
        -- Surrogate key
        
  
    
    (hash(concat_ws('|', account_id, first_seen_utc)) & 9223372036854775807)::bigint
  
 as account_sk,
        
        -- Attributes
        account_id,
        
        -- Effective timestamp
        first_seen_utc as effective_ts
        
    from distinct_accounts
)

select * from final
), staged as (
    select * from __dbt__cte__int_account_current
)






  



-- Incremental run: Insert new/changed records, close old versions

-- New records and updated versions
select
    source.account_sk,
    source.account_bk,
    
    source.account_id,
    
    source.effective_ts as valid_from_utc,
    
  
    '9999-12-31 23:59:59'::timestamp
  
 as valid_to_utc,
    true as is_current
from staged as source
left join "dev"."main_marts"."dim_account" as existing
    on source.account_bk = existing.account_bk
    and existing.is_current = true
where
    -- New business key (no existing current record)
    existing.account_bk is null
    -- Or attributes have changed
    or (coalesce(cast(source.account_id as varchar), '') != coalesce(cast(existing.account_id as varchar), ''))

union all

-- Existing current records that need to be closed (attributes changed)
select
    existing.account_sk,
    existing.account_bk,
    
    existing.account_id,
    
    existing.valid_from_utc,
    source.effective_ts as valid_to_utc,  -- Close at the time of change
    false as is_current
from "dev"."main_marts"."dim_account" as existing
inner join staged as source
    on source.account_bk = existing.account_bk
where
    existing.is_current = true
    and (coalesce(cast(source.account_id as varchar), '') != coalesce(cast(existing.account_id as varchar), ''))


