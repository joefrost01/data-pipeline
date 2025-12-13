{{
  config(
    materialized='view'
  )
}}

{#
  Staging model for load metadata with derived fields.
  
  Extracts from filename:
  - feed_name: Stable identifier (e.g., "futures_order_events")
  - business_date: The date this data represents
  
  Filename patterns supported:
  - feed_YYYYMMDD.csv → explicit date
  - feed_YYYYMMDD_v2.csv → explicit date with version suffix (ignored, use load order)
  - feed_YYYYMMDD_correction.csv → explicit date with correction marker
  - feed.csv → no date, infer T-1 from load time
  
  The combination of (feed_name, business_date, loaded_at) lets us determine
  which load represents the latest version of truth for any given feed + day.
#}

with source as (
    select * from {{ source('raw', 'load_metadata') }}
),

-- Extract components from filename using regex
parsed as (
    select
        load_id,
        filename,
        table_name,
        row_count,
        started_at,
        completed_at as loaded_at,
        
        -- Extract feed name: everything before _YYYYMMDD or .csv
        -- Pattern: strip date patterns and extensions
        {% if target.type == 'bigquery' %}
        regexp_replace(
            regexp_replace(filename, r'_\d{8}.*\.csv$', ''),
            r'\.csv$', ''
        ) as feed_name,
        
        -- Extract business date if present (YYYYMMDD pattern)
        case
            when regexp_contains(filename, r'_(\d{8})')
            then parse_date('%Y%m%d', regexp_extract(filename, r'_(\d{8})'))
            else date_sub(date(completed_at), interval 1 day)  -- T-1 fallback
        end as business_date
        
        {% elif target.type == 'duckdb' %}
        regexp_replace(
            regexp_replace(filename, '_\d{8}.*\.csv$', ''),
            '\.csv$', ''
        ) as feed_name,
        
        -- Extract business date if present (YYYYMMDD pattern)
        case
            when regexp_matches(filename, '_(\d{8})')
            then strptime(regexp_extract(filename, '_(\d{8})', 1), '%Y%m%d')::date
            else (completed_at::date - interval '1 day')::date  -- T-1 fallback
        end as business_date
        
        {% else %}
        -- Fallback for other databases
        replace(replace(filename, '.csv', ''), '_', '') as feed_name,
        completed_at::date - interval '1 day' as business_date
        {% endif %}
        
    from source
),

-- Add version ranking within feed + business_date
with_version as (
    select
        *,
        row_number() over (
            partition by feed_name, business_date
            order by loaded_at asc
        ) as load_sequence,
        
        row_number() over (
            partition by feed_name, business_date
            order by loaded_at desc
        ) as reverse_sequence
        
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
    load_sequence,
    reverse_sequence = 1 as is_latest_for_business_date
    
from with_version
