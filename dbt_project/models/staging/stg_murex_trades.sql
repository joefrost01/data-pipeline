{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for Murex trades.
    Cleans and types raw data from external table.
*/

select
    trade_id as source_trade_id,
    'MUREX' as source_system,
    cast(trade_time as timestamp) as trade_time,
    cast(counterparty as string) as counterparty_name,
    cast(trader_id as string) as trader_id,
    cast(instrument as string) as instrument_id,
    upper(cast(side as string)) as side,
    cast(quantity as int64) as quantity,
    cast(price as numeric) as price,
    cast(book_id as string) as book_id,
    current_timestamp() as loaded_at

from {{ source('raw', 'murex_trades') }}

where trade_id is not null
  and trade_time is not null
  and side in ('BUY', 'SELL')
  and quantity > 0
