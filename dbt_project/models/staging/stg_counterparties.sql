{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for counterparty reference data.
    Cleans and types raw data from reference source.
*/

select
    cast(counterparty_id as string) as counterparty_id,
    cast(counterparty_name as string) as counterparty_name,
    cast(lei as string) as lei,
    cast(country as string) as country,
    cast(status as string) as status,
    current_timestamp() as loaded_at

from {{ source('reference', 'counterparties') }}
where counterparty_id is not null
