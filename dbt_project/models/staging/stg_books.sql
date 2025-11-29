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
    cast(book_id as string) as book_id,
    cast(book_name as string) as book_name,
    cast(desk_id as string) as desk_id,
    cast(legal_entity as string) as legal_entity,
    current_timestamp() as loaded_at

from {{ source('reference', 'books') }}
where book_id is not null
