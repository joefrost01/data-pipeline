{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Venue A trades.
    Maps venue-specific field names to canonical schema.
    
    NOTE: Venue A uses 'B'/'S' for side values. We normalise these to 'BUY'/'SELL'
    to match the canonical schema used by all other sources.
*/

select
    exec_id as source_trade_id,
    'VENUE_A' as source_system,
    cast(exec_timestamp as timestamp) as trade_time,
    {{ typed_cast('null', 'STRING') }} as counterparty_name,  -- Venue A doesn't provide
    {{ typed_cast('trader', 'STRING') }} as trader_id,
    {{ typed_cast('symbol', 'STRING') }} as instrument_id,
    -- Normalise side: Venue A uses B/S, we use BUY/SELL
    case upper({{ typed_cast('side', 'STRING') }})
        when 'B' then 'BUY'
        when 'BUY' then 'BUY'
        when 'S' then 'SELL'
        when 'SELL' then 'SELL'
        else upper({{ typed_cast('side', 'STRING') }})  -- Pass through unknown values for visibility
    end as side,
    cast(qty as int64) as quantity,
    cast(px as numeric) as price,
    cast(null as string) as book_id,  -- Enriched later from trader mapping
    {{ now_ts() }} as loaded_at


from {{ source('raw', 'venue_a_trades') }}

where exec_id is not null
  and exec_timestamp is not null
  and upper(side) in ('BUY', 'SELL', 'B', 'S')
  and qty > 0
