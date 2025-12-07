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
    {{ typed_cast('counterparty_id', 'STRING') }} as counterparty_id,
    {{ typed_cast('counterparty_name', 'STRING') }} as counterparty_name,
    {{ typed_cast('lei', 'STRING') }} as lei,
    {{ typed_cast('country', 'STRING') }} as country,
    {{ typed_cast('status', 'STRING') }} as status,
    {{ now_ts() }} as loaded_at

from {{ source('reference', 'counterparties') }}
where counterparty_id is not null