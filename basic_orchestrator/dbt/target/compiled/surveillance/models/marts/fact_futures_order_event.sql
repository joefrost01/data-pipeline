



with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
    
    where _load_id not in (select distinct _load_id from "dev"."main_marts"."fact_futures_order_event")
    
),

-- Build dimensional keys for joining
with_keys as (
    select
        e.*,
        
        -- Event business key (identifies unique real-world event)
        e.order_id || '|' || cast(e.event_seq as varchar) as event_bk,
        
        -- Surrogate key includes loaded_at for bi-temporal versioning
        
  
    
    (hash(concat_ws('|', e.order_id, e.event_seq, e.loaded_at)) & 9223372036854775807)::bigint
  
 as fact_event_sk,
        
        -- Extra dimension key (-1 if no schema drift)
        case
            when e._extra is not null then 
  
    
    (hash(concat_ws('|', e._extra)) & 9223372036854775807)::bigint
  

            else cast(-1 as bigint)
        end as extra_sk,

        -- Date and time keys
        
  
    cast(strftime(e.event_timestamp_utc, '%Y%m%d') as bigint)
  
 as date_key,
        
  
    cast(strftime(e.event_timestamp_utc, '%H%M%S') as bigint)
  
 as time_key,
        
  
    cast(epoch_ms(e.event_timestamp_utc) % 1000 as bigint)
  
 as event_millis,

        -- Build business keys for dimension lookups
        'Commercial Markets' || '|' ||
        case e.desk
            when 'Commodities' then 'FICC'
            when 'Rates' then 'FICC'
            when 'Macro' then 'FICC'
            when 'Equities' then 'EQUITIES'
            else 'OTHER'
        end || '|' || e.desk || '|' || e.book as org_bk,

        e.trader_id as trader_bk,
        e.account_id as account_bk,
        e.exchange || ':' || e.symbol || ':' || e.contract_month as instrument_bk

    from events e
),

-- Join to dimensions
with_dimensions as (
    select
        k.fact_event_sk,
        k.event_bk,
        k.date_key,
        k.time_key,
        k.event_millis,
        k.event_timestamp_utc,

        -- Bi-temporal fields: new rows start as "current"
        k.loaded_at as valid_from_utc,
        
  
    '9999-12-31 23:59:59'::timestamp
  
 as valid_to_utc,
        true as is_current,
        k.business_date,

        -- Order dimension (Type 1 - simple join)
        o.order_sk,
        k.event_seq,

        -- SCD2 dimension lookups (current version)
        org.org_sk,
        t.trader_sk,
        a.account_sk,
        i.instrument_sk,

        -- Extra dimension (sparse, for schema drift)
        k.extra_sk,

        -- Event attributes
        k.event_type,
        k.order_status,

        -- Fill information
        k.filled_quantity,
        k.remaining_quantity,
        k.last_fill_qty,
        k.last_fill_price,
        k.avg_fill_price,

        -- Market context
        k.best_bid,
        k.best_ask,
        k.mid_price,
        k.spread,

        -- Risk information
        k.pre_trade_risk_check,
        k.risk_limit_id,
        k.current_position,
        k.max_position_limit,
        k.reject_reason,

        -- Lineage
        k._load_id,
        k.feed_name

    from with_keys k

    -- Order dimension
    left join "dev"."main_marts"."dim_order" o
        on k.order_id = o.order_id

    -- Org dimension (SCD2 - join to current)
    left join "dev"."main_marts"."dim_org" org
        on k.org_bk = org.org_bk
        and org.is_current = true

    -- Trader dimension (SCD2)
    left join "dev"."main_marts"."dim_trader" t
        on k.trader_bk = t.trader_bk
        and t.is_current = true

    -- Account dimension (SCD2)
    left join "dev"."main_marts"."dim_account" a
        on k.account_bk = a.account_bk
        and a.is_current = true

    -- Instrument dimension (SCD2)
    left join "dev"."main_marts"."dim_instrument" i
        on k.instrument_bk = i.instrument_bk
        and i.is_current = true
)

select
    -- Surrogate key
    fact_event_sk,

    -- Business key
    event_bk,

    -- Bi-temporal fields
    valid_from_utc,
    valid_to_utc,
    is_current,
    business_date,

    -- Dimensional keys
    order_sk,
    event_seq,
    date_key,
    time_key,
    event_millis,
    event_timestamp_utc,
    org_sk,
    trader_sk,
    account_sk,
    instrument_sk,
    extra_sk,

    -- Event
    event_type,
    order_status,

    -- Fills
    filled_quantity,
    remaining_quantity,
    last_fill_qty,
    last_fill_price,
    avg_fill_price,

    -- Market
    best_bid,
    best_ask,
    mid_price,
    spread,

    -- Risk
    pre_trade_risk_check,
    risk_limit_id,
    current_position,
    max_position_limit,
    reject_reason,

    -- Lineage
    _load_id,
    feed_name

from with_dimensions