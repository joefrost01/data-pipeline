{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for trader reference data.
    Cleans and types raw data from reference source.
*/

select
    {{ typed_cast('trader_id', 'STRING') }} as trader_id,
    {{ typed_cast('trader_name', 'STRING') }} as trader_name,
    {{ typed_cast('desk_id', 'STRING') }} as desk_id,
    {{ typed_cast('status', 'STRING') }} as status,
    {{ typed_cast('compliance_officer', 'STRING') }} as compliance_officer,
    {{ now_ts() }} as loaded_at

from {{ source('reference', 'traders') }}
where trader_id is not null