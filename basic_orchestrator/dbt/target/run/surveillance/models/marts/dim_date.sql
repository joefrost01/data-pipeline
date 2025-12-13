
  
    
    

    create  table
      "dev"."main_marts"."dim_date__dbt_tmp"
  
    as (
      





-- DuckDB: Use generate_series
with date_spine as (
    select date_day::date as date_day
    from generate_series(
        date '2020-01-01',
        date '2030-12-31',
        interval '1 day'
    ) as t(date_day)
)



select
    -- Natural key (YYYYMMDD)
    
  
    cast(strftime(date_day, '%Y%m%d') as bigint)
  
 as date_key,
    
    -- Date value
    date_day as date_utc,
    
    -- Calendar attributes
    
    extract(year from date_day)::int as year,
    extract(quarter from date_day)::int as quarter,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day_of_month,
    extract(dow from date_day)::int as day_of_week,
    extract(week from date_day)::int as iso_week,
    extract(dow from date_day) in (0, 6) as is_weekend
    

from date_spine
    );
  
  