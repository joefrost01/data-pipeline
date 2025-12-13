





-- DuckDB: Generate series
with seconds as (
    select generate_series::integer as second_of_day
    from generate_series(0, 86399)
)



select
    -- Natural key (HHMMSS)
    
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
    

from seconds