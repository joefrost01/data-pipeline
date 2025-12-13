

with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
),

-- Derive division from desk (as agreed)
with_derived as (
    select
        desk,
        book,
        'Commercial Markets' as legal_entity,
        case desk
            when 'Commodities' then 'FICC'
            when 'Rates' then 'FICC'
            when 'Macro' then 'FICC'
            when 'Equities' then 'EQUITIES'
            else 'OTHER'
        end as division,
        min(event_timestamp_utc) as first_seen_utc,
        max(event_timestamp_utc) as last_seen_utc
    from events
    group by desk, book
),

-- Build business key and surrogate key
final as (
    select
        -- Business key (pipe-delimited for readability)
        legal_entity || '|' || division || '|' || desk || '|' || book as org_bk,
        
        -- Surrogate key
        
  
    
    (hash(concat_ws('|', legal_entity, division, desk, book, first_seen_utc)) & 9223372036854775807)::bigint
  
 as org_sk,
        
        -- Attributes
        legal_entity,
        division,
        desk,
        book,
        
        -- Effective timestamp (first time we saw this combination)
        first_seen_utc as effective_ts
        
    from with_derived
)

select * from final