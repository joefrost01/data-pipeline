{{
    config(
        materialized='table'
    )
}}

/*
    Instrument dimension (SCD Type 1 - current state only).
    
    We use SCD-1 here because:
    - Millions of instruments across asset classes
    - Frequent minor attribute updates on OTC instruments
    - SCD-2 would multiply storage significantly
    - Most analytics need current attributes
    
    Historical needs are met by:
    - Trade snapshots capture instrument attributes at trade time
    - Separate instrument_history table for instrument-specific analysis
*/

select
    {{ typed_cast('instrument_id', 'STRING') }} as instrument_id,
    {{ typed_cast('symbol', 'STRING') }} as symbol,
    {{ typed_cast('isin', 'STRING') }} as isin,
    {{ typed_cast('asset_class', 'STRING') }} as asset_class,
    {{ typed_cast('currency', 'STRING') }} as currency,
    {{ now_ts() }} as updated_at

from {{ source('reference', 'instruments') }}
where instrument_id is not null