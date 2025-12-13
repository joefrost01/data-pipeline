{{
  config(
    materialized='table'
  )
}}

{#
  Time dimension with natural integer key (HHMMSS).
  
  One row for each second of the day (86,400 rows).
  Milliseconds are stored separately in the fact table.
#}

{% if target.type == 'bigquery' %}

-- BigQuery: Generate seconds of day
with seconds as (
    select second_of_day
    from unnest(generate_array(0, 86399)) as second_of_day
)

{% elif target.type == 'duckdb' %}

-- DuckDB: Generate series
with seconds as (
    select generate_series::integer as second_of_day
    from generate_series(0, 86399)
)

{% else %}

-- Fallback: Recursive CTE
with recursive seconds as (
    select 0 as second_of_day
    union all
    select second_of_day + 1
    from seconds
    where second_of_day < 86399
)

{% endif %}

select
    -- Natural key (HHMMSS)
    {% if target.type == 'duckdb' %}
    cast(
        lpad(cast(second_of_day // 3600 as varchar), 2, '0') ||
        lpad(cast((second_of_day % 3600) // 60 as varchar), 2, '0') ||
        lpad(cast(second_of_day % 60 as varchar), 2, '0')
        as bigint
    ) as time_key,
    
    -- Time components
    second_of_day // 3600 as hour,
    (second_of_day % 3600) // 60 as minute,
    second_of_day % 60 as second
    {% else %}
    cast(
        lpad(cast(second_of_day / 3600 as varchar), 2, '0') ||
        lpad(cast((second_of_day % 3600) / 60 as varchar), 2, '0') ||
        lpad(cast(second_of_day % 60 as varchar), 2, '0')
        as {% if target.type == 'bigquery' %}int64{% else %}bigint{% endif %}
    ) as time_key,
    
    -- Time components
    second_of_day / 3600 as hour,
    (second_of_day % 3600) / 60 as minute,
    second_of_day % 60 as second
    {% endif %}

from seconds
