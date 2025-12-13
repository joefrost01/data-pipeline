{{
  config(
    materialized='table'
  )
}}

{#
  Date dimension with natural integer key (YYYYMMDD).
  
  Generated to cover a reasonable range for futures trading.
  Expands as needed when new dates appear in the data.
#}

{% if target.type == 'bigquery' %}

-- BigQuery: Use GENERATE_DATE_ARRAY
with date_spine as (
    select date_day
    from unnest(
        generate_date_array('2020-01-01', '2030-12-31', interval 1 day)
    ) as date_day
)

{% elif target.type == 'duckdb' %}

-- DuckDB: Use generate_series
with date_spine as (
    select date_day::date as date_day
    from generate_series(
        date '2020-01-01',
        date '2030-12-31',
        interval '1 day'
    ) as t(date_day)
)

{% else %}

-- Fallback: Recursive CTE
with recursive date_spine as (
    select cast('2020-01-01' as date) as date_day
    union all
    select date_day + interval '1 day'
    from date_spine
    where date_day < '2030-12-31'
)

{% endif %}

select
    -- Natural key (YYYYMMDD)
    {{ date_key_from_ts('date_day') }} as date_key,
    
    -- Date value
    date_day as date_utc,
    
    -- Calendar attributes
    {% if target.type == 'bigquery' %}
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,
    extract(isoweek from date_day) as iso_week,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
    {% elif target.type == 'duckdb' %}
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day_of_month,
    extract(dow from date_day)::int as day_of_week,
    extract(week from date_day)::int as iso_week,
    extract(dow from date_day) in (0, 6) as is_weekend
    {% else %}
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    extract(week from date_day) as iso_week,
    extract(dow from date_day) in (0, 6) as is_weekend
    {% endif %}

from date_spine
