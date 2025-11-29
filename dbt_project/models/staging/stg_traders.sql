{{
    config(
        materialized='view'
    )
}}

select
    cast(trader_id as string) as trader_id,
    cast(trader_name as string) as trader_name,
    cast(desk_id as string) as desk_id,
    cast(status as string) as status,
    cast(compliance_officer as string) as compliance_officer,
    current_timestamp() as loaded_at

from {{ source('reference', 'traders') }}
where trader_id is not null
