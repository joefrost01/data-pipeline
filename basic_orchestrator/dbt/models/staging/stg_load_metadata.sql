{{
    config(
        materialized='view',
        schema=var('staging_dataset', 'surveillance_staging')
    )
}}

{#
    Staged load metadata with derived fields.
    
    Extracts from filename:
    - feed_name: Stable identifier (e.g., 'futures_orders_events')
    - business_date: The date this data represents
#}

with source as (
    select * from {{ source('raw', 'load_metadata') }}
),

parsed as (
    select
        load_id,
        filename,
        table_name,
        row_count,
        started_at,
        completed_at as loaded_at,
        
        -- Extract feed name (strip date suffix and extension)
        {% if target.type == 'duckdb' %}
        regexp_replace(regexp_replace(filename, '_\d{8}.*\.csv$', ''), '\.csv$', '') as feed_name,
        {% else %}
        regexp_replace(regexp_replace(filename, r'_\d{8}.*\.csv$', ''), r'\.csv$', '') as feed_name,
        {% endif %}
        
        -- Extract business date from filename, or default to day before load
        {% if target.type == 'duckdb' %}
        case
            when regexp_matches(filename, '_\d{8}')
            then strptime(regexp_extract(filename, '_(\d{8})', 1), '%Y%m%d')::date
            else (completed_at::date - interval '1 day')::date
        end as business_date
        {% else %}
        case
            when regexp_contains(filename, r'_\d{8}')
            then parse_date('%Y%m%d', regexp_extract(filename, r'_(\d{8})'))
            else date_sub(date(completed_at), interval 1 day)
        end as business_date
        {% endif %}
        
    from source
),

with_version as (
    select
        *,
        row_number() over (
            partition by feed_name, business_date 
            order by loaded_at desc
        ) as version_rank
    from parsed
)

select
    load_id,
    filename,
    table_name,
    row_count,
    started_at,
    loaded_at,
    feed_name,
    business_date,
    version_rank = 1 as is_latest
from with_version
