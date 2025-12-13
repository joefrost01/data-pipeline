

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