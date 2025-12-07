{{
    config(
        materialized='table',
        partition_by={
            'field': 'trade_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['trader_id']
    )
}}

/*
    Surveillance extract: the main output for the surveillance partner.
    
    Contains 7-day rolling window of enriched trades in the format
    expected by the partner's MAR detection system.

*/

select
    -- Identity
    trade_id,
    source_trade_id,
    source_system,
    
    -- Timing
    trade_time,
    trade_date,
    
    -- Trade details
    side,
    quantity,
    price,
    notional,
    
    -- Trader
    trader_id,
    trader_name,
    desk_id,
    compliance_officer,
    
    -- Counterparty
    counterparty_id,
    counterparty_name,
    counterparty_lei,
    
    -- Instrument
    instrument_id,
    instrument_symbol,
    isin,
    asset_class,
    currency,
    
    -- Book
    book_id,
    book_name,
    legal_entity,
    
    -- Audit
    loaded_at,
    enriched_at,
    {{ now_ts() }} as extracted_at

from {{ ref('trades_enriched') }}

where trade_date >= {{ date_sub_days(today_date(), 7) }}