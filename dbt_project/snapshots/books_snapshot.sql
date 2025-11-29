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
    book_id,
    book_name,
    desk_id,
    legal_entity
from {{ ref('stg_books') }}

{% endsnapshot %}
