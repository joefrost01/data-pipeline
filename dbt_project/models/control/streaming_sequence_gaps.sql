{{
    config(
        materialized='table',
        partition_by={
            'field': 'detected_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['source_system']
    )
}}

/*
    Detects gaps in streaming message sequences.
    
    When sources provide sequential IDs, we can identify missing messages
    by looking for non-consecutive sequences within each partition.
*/

with sequences as (
    select
        'RFQ_STREAM' as source_system,
        _kafka_partition as partition_id,
        source_sequence_id,
        lag(source_sequence_id) over (
            partition by _kafka_partition
            order by source_sequence_id
        ) as prev_sequence_id,
        _ingestion_time
    from {{ ref('stg_rfqs') }}
    where source_sequence_id is not null
      and _ingestion_time >= timestamp_sub(current_timestamp(), interval 7 day)
),

gaps as (
    select
        source_system,
        partition_id,
        prev_sequence_id + 1 as gap_start,
        source_sequence_id - 1 as gap_end,
        source_sequence_id - prev_sequence_id - 1 as gap_size,
        _ingestion_time as detected_at
    from sequences
    where source_sequence_id - prev_sequence_id > 1
      and prev_sequence_id is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['source_system', 'partition_id', 'gap_start']) }} as gap_id,
    detected_at,
    source_system,
    partition_id,
    gap_start,
    gap_end,
    gap_size,
    case
        when gap_size > 1000 then 'CRITICAL'
        when gap_size > 100 then 'HIGH'
        when gap_size > 10 then 'MEDIUM'
        else 'LOW'
    end as severity,
    cast(null as timestamp) as resolved_at,
    cast(null as string) as resolution_type

from gaps
order by detected_at desc
