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
    cast(instrument_id as string) as instrument_id,
    cast(symbol as string) as symbol,
    cast(isin as string) as isin,
    cast(asset_class as string) as asset_class,
    cast(currency as string) as currency,
    current_timestamp() as updated_at

from {{ source('reference', 'instruments') }}
where instrument_id is not null
