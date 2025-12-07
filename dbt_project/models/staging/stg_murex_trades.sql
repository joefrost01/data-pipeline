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
    {{ typed_cast('counterparty', 'STRING') }} as counterparty_name,
    {{ typed_cast('trader_id', 'STRING') }} as trader_id,
    {{ typed_cast('instrument', 'STRING') }} as instrument_id,
    upper({{ typed_cast('side', 'STRING') }}) as side,
    {{ typed_cast('quantity', 'INT64') }} as quantity,
    cast(price as numeric) as price,
    {{ typed_cast('book_id', 'STRING') }} as book_id,
    {{ now_ts() }} as loaded_at

from {{ source('raw', 'murex_trades') }}

where trade_id is not null
  and trade_time is not null
  and side in ('BUY', 'SELL')
  and quantity > 0
