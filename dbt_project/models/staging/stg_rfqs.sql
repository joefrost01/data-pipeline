{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for streaming RFQs.
    Deduplicates at-least-once delivery using event_id + ingestion_time.
*/

with deduplicated as (
    select
        *,
        row_number() over (
            partition by rfq_id
            order by _ingestion_time asc
        ) as _row_num
    from {{ source('raw', 'rfq_stream') }}
    where rfq_id is not null
      and rfq_timestamp is not null
)

select
    rfq_id,
    cast(rfq_timestamp as timestamp) as rfq_timestamp,
    cast(trader_id as string) as trader_id,
    cast(instrument as string) as instrument_id,
    upper(cast(side as string)) as side,
    cast(quantity as int64) as quantity,
    cast(price as numeric) as price,
    cast(counterparty_id as string) as counterparty_id,
    source_sequence_id,
    _kafka_partition,
    _kafka_offset,
    _ingestion_time as loaded_at

from deduplicated
where _row_num = 1
  and side in ('BUY', 'SELL')
  and quantity > 0
