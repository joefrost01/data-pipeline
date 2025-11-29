{{
    config(
        materialized='table',
        partition_by={
            'field': 'business_date',
            'data_type': 'date'
        },
        cluster_by=['source_name']
    )
}}

/*
    Source completeness tracking.
    
    Monitors whether expected sources have delivered data each day.
    Used for alerting and SLA reporting.
*/

with expected_sources as (
    select * from {{ ref('expected_sources') }}
),

actual_arrivals as (
    select
        source_name,
        date(run_timestamp) as business_date,
        min(run_timestamp) as first_file_at,
        max(run_timestamp) as last_file_at,
        count(*) as file_count,
        sum(row_count) as total_rows
    from {{ source('control', 'validation_runs') }}
    where passed = true
    group by source_name, date(run_timestamp)
),

-- Calculate consecutive missing days
missing_streak as (
    select
        e.source_name,
        d.date_value as business_date,
        case when a.source_name is null then 1 else 0 end as is_missing
    from expected_sources e
    cross join unnest(generate_date_array(
        date_sub(current_date(), interval 30 day),
        current_date()
    )) as d(date_value)
    left join actual_arrivals a
        on e.source_name = a.source_name
        and d.date_value = a.business_date
),

streaks as (
    select
        source_name,
        business_date,
        sum(case when is_missing = 0 then 1 else 0 end) over (
            partition by source_name
            order by business_date
            rows between unbounded preceding and current row
        ) as reset_group
    from missing_streak
),

consecutive_missing as (
    select
        source_name,
        business_date,
        row_number() over (
            partition by source_name, reset_group
            order by business_date
        ) - 1 as consecutive_missing_days
    from streaks
)

select
    coalesce(a.business_date, current_date()) as business_date,
    e.source_name,
    e.expected_by,
    a.first_file_at,
    a.last_file_at,
    coalesce(a.file_count, 0) as file_count,
    a.total_rows,
    case
        when a.source_name is null then 'MISSING'
        when a.first_file_at > timestamp(concat(cast(a.business_date as string), ' ', e.expected_by)) then 'LATE'
        when a.file_count < e.min_files_per_day then 'PARTIAL'
        else 'COMPLETE'
    end as status,
    cm.consecutive_missing_days,
    current_timestamp() as checked_at

from expected_sources e
left join actual_arrivals a
    on e.source_name = a.source_name
    and a.business_date = current_date()
left join consecutive_missing cm
    on e.source_name = cm.source_name
    and cm.business_date = current_date()
