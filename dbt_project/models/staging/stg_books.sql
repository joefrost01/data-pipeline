{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for trading book reference data.
    Cleans and types raw data from reference source.
*/

select
    {{ typed_cast('book_id', 'STRING') }} as book_id,
    {{ typed_cast('book_name', 'STRING') }} as book_name,
    {{ typed_cast('desk_id', 'STRING') }} as desk_id,
    {{ typed_cast('legal_entity', 'STRING') }} as legal_entity,
    {{ now_ts() }} as loaded_at

from {{ source('reference', 'books') }}
where book_id is not null