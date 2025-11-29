{% snapshot books_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='book_id',
        strategy='check',
        check_cols=['book_name', 'desk_id', 'legal_entity'],
        invalidate_hard_deletes=true
    )
}}

/*
    Book dimension snapshot (SCD Type 2).
    
    Tracks historical changes for:
    - Book hierarchy restructuring
    - Legal entity migrations
*/

select
    cast(book_id as string) as book_id,
    cast(book_name as string) as book_name,
    cast(desk_id as string) as desk_id,
    cast(legal_entity as string) as legal_entity
from {{ source('reference', 'books') }}
where book_id is not null

{% endsnapshot %}
