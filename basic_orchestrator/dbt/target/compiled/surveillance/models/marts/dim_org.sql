



with  __dbt__cte__int_org_current as (


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
), staged as (
    select * from __dbt__cte__int_org_current
)






  

  

  

  



-- Incremental run: Insert new/changed records, close old versions

-- New records and updated versions
select
    source.org_sk,
    source.org_bk,
    
    source.legal_entity,
    
    source.division,
    
    source.desk,
    
    source.book,
    
    source.effective_ts as valid_from_utc,
    
  
    '9999-12-31 23:59:59'::timestamp
  
 as valid_to_utc,
    true as is_current
from staged as source
left join "dev"."main_marts"."dim_org" as existing
    on source.org_bk = existing.org_bk
    and existing.is_current = true
where
    -- New business key (no existing current record)
    existing.org_bk is null
    -- Or attributes have changed
    or (coalesce(cast(source.legal_entity as varchar), '') != coalesce(cast(existing.legal_entity as varchar), '') or coalesce(cast(source.division as varchar), '') != coalesce(cast(existing.division as varchar), '') or coalesce(cast(source.desk as varchar), '') != coalesce(cast(existing.desk as varchar), '') or coalesce(cast(source.book as varchar), '') != coalesce(cast(existing.book as varchar), ''))

union all

-- Existing current records that need to be closed (attributes changed)
select
    existing.org_sk,
    existing.org_bk,
    
    existing.legal_entity,
    
    existing.division,
    
    existing.desk,
    
    existing.book,
    
    existing.valid_from_utc,
    source.effective_ts as valid_to_utc,  -- Close at the time of change
    false as is_current
from "dev"."main_marts"."dim_org" as existing
inner join staged as source
    on source.org_bk = existing.org_bk
where
    existing.is_current = true
    and (coalesce(cast(source.legal_entity as varchar), '') != coalesce(cast(existing.legal_entity as varchar), '') or coalesce(cast(source.division as varchar), '') != coalesce(cast(existing.division as varchar), '') or coalesce(cast(source.desk as varchar), '') != coalesce(cast(existing.desk as varchar), '') or coalesce(cast(source.book as varchar), '') != coalesce(cast(existing.book as varchar), ''))


