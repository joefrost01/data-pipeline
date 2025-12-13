{#
  Extract distinct instruments from events.
  
  Business key: exchange:symbol:contract_month
  
  Instrument attributes are fairly stable (multiplier, tick_size, currency)
  so we take the most recent values seen.
#}

with events as (
    select * from {{ ref('stg_futures_order_events') }}
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
        {{ sk(["exchange", "symbol", "contract_month", "first_seen_utc"]) }} as instrument_sk,
        
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
