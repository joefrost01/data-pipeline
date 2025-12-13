{#
  Extract distinct accounts from events.
  
  Business key: account_id
  
  Minimal attributes since we're deriving from events only.
  Additional attributes would come from a reference feed.
#}

with events as (
    select * from {{ ref('stg_futures_order_events') }}
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
        {{ sk(["account_id", "first_seen_utc"]) }} as account_sk,
        
        -- Attributes
        account_id,
        
        -- Effective timestamp
        first_seen_utc as effective_ts
        
    from distinct_accounts
)

select * from final
