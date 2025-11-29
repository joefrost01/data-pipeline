{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Venue A trades.
    Maps venue-specific field names to canonical schema.
*/

select
    exec_id as source_trade_id,
    'VENUE_A' as source_system,
    cast(exec_timestamp as timestamp) as trade_time,
    cast(null as string) as counterparty_name,  -- Venue A doesn't provide
    cast(trader as string) as trader_id,
    cast(symbol as string) as instrument_id,
    upper(cast(side as string)) as side,
    cast(qty as int64) as quantity,
    cast(px as numeric) as price,
    cast(null as string) as book_id,  -- Enriched later from trader mapping
    current_timestamp() as loaded_at

from {{ source('raw', 'venue_a_trades') }}

where exec_id is not null
  and exec_timestamp is not null
  and side in ('BUY', 'SELL', 'B', 'S')
  and qty > 0
