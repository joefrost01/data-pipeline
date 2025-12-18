{#
    For bi-temporal: always INSERT new rows, never update.
    - DuckDB: 'append' strategy
    - BigQuery: 'merge' without unique_key = pure insert
    Post-hook then closes prior versions.
#}
{{
    config(
        materialized='incremental',
        schema=var('curated_dataset', 'event_interaction'),
        incremental_strategy='merge' if target.type == 'bigquery' else 'append',
        post_hook="{{ close_prior_order_versions() }}",
        full_refresh=false,
        on_schema_change='append_new_columns',
        **table_config(
            partition_field='ExtractDate',
            cluster_fields=['is_current']
        )
    )
}}

{#
    Curated futures order model - One Big Table with nested structs.
    
    Transforms event-level rows into order-level rows with Events array.
    Bi-temporal at order level - whole order versioned when correction arrives.
    
    Input:  stg_futures_order_events (one row per event)
    Output: order_financial_markets (one row per order with Events[] array)
    
    Structure matches the JSON format:
    - Type: "Master.Transaction"
    - Execution STRUCT (with nested Economics, Identifier, Events[])
    - Unit STRUCT
    - Bi-temporal fields
#}

with staged_events as (
    select * from {{ ref('stg_futures_order_events') }}
    {% if is_incremental() %}
    where _load_id not in (select distinct _load_id from {{ this }})
    {% endif %}
),

-- Aggregate events by order, building the Events array
orders_with_events as (
    select
        -- Order-level fields (same for all events in an order)
        extract_date,
        order_message_id,
        order_start_date,
        order_end_date,
        trader_id,
        
        -- Instrument (same for all events)
        instrument_exchange,
        instrument_ticker,
        instrument_month_code,
        instrument_year_code,
        
        -- Economics (same for all events)
        economics_amount,
        economics_contract_size,
        order_price,
        price_currency,
        
        -- Unit (same for all events)
        book_name,
        business_unit,
        region,
        
        -- Load tracking
        _load_id,
        max(loaded_at) as loaded_at,
        
        -- Aggregate events into array of structs
        {% if is_duckdb() %}
        list(
            struct_pack(
                "Type" := event_type,
                "EventID" := concat(order_message_id, '-', cast(event_seq as varchar)),
                "EventDateTime" := event_date_time,
                "Price" := event_price,
                "PriceType" := event_price_type,
                "Amount" := event_amount,
                "Counterparty" := struct_pack(
                    "Type" := counterparty_type,
                    "CounterpartyName" := counterparty_name,
                    "CounterpartyCode" := counterparty_code
                )
            )
            order by event_seq
        ) as events_array
        {% else %}
        array_agg(
            struct(
                event_type as Type,
                concat(order_message_id, '-', cast(event_seq as string)) as EventID,
                event_date_time as EventDateTime,
                event_price as Price,
                event_price_type as PriceType,
                event_amount as Amount,
                struct(
                    counterparty_type as Type,
                    counterparty_name as CounterpartyName,
                    counterparty_code as CounterpartyCode
                ) as Counterparty
            )
            order by event_seq
        ) as events_array
        {% endif %}
        
    from staged_events
    group by
        extract_date,
        order_message_id,
        order_start_date,
        order_end_date,
        trader_id,
        instrument_exchange,
        instrument_ticker,
        instrument_month_code,
        instrument_year_code,
        economics_amount,
        economics_contract_size,
        order_price,
        price_currency,
        book_name,
        business_unit,
        region,
        _load_id
),

-- Build the final nested structure
final as (
    select
        -- Type
        'Master.Transaction' as "Type",
        
        -- Extract date
        extract_date as "ExtractDate",
        
        -- Execution struct (deeply nested)
        {% if is_duckdb() %}
        struct_pack(
            "Type" := 'Execution.OrderStream',
            "OrderMessageID" := order_message_id,
            "OrderStartDate" := order_start_date,
            "OrderEndDate" := order_end_date,
            "TraderID" := trader_id,
            "Economics" := struct_pack(
                "Type" := 'IR.RateFuture',
                "AssetType" := 'Interest Rate Futures',
                "Amount" := economics_amount,
                "ContractSize" := economics_contract_size,
                "Price" := order_price,
                "PriceCurrency" := struct_pack(
                    "Type" := 'ID.ISO',
                    "ISOCode" := price_currency
                )
            ),
            "Identifier" := struct_pack(
                "Type" := 'ID.FutureExchange',
                "Exchange" := instrument_exchange,
                "Ticker" := instrument_ticker,
                "MonthCode" := instrument_month_code,
                "YearCode" := instrument_year_code
            ),
            "Events" := events_array
        ) as "Execution",
        {% else %}
        struct(
            'Execution.OrderStream' as Type,
            order_message_id as OrderMessageID,
            order_start_date as OrderStartDate,
            order_end_date as OrderEndDate,
            trader_id as TraderID,
            struct(
                'IR.RateFuture' as Type,
                'Interest Rate Futures' as AssetType,
                economics_amount as Amount,
                economics_contract_size as ContractSize,
                order_price as Price,
                struct(
                    'ID.ISO' as Type,
                    price_currency as ISOCode
                ) as PriceCurrency
            ) as Economics,
            struct(
                'ID.FutureExchange' as Type,
                instrument_exchange as Exchange,
                instrument_ticker as Ticker,
                instrument_month_code as MonthCode,
                instrument_year_code as YearCode
            ) as Identifier,
            events_array as Events
        ) as "Execution",
        {% endif %}
        
        -- Unit struct
        {% if is_duckdb() %}
        struct_pack(
            "Type" := 'Unit.Book',
            "BookName" := book_name,
            "BusinessUnit" := business_unit,
            "Region" := region
        ) as "Unit",
        {% else %}
        struct(
            'Unit.Book' as Type,
            book_name as BookName,
            business_unit as BusinessUnit,
            region as Region
        ) as "Unit",
        {% endif %}
        
        -- Bi-temporal fields
        coalesce(loaded_at, {{ now_ts() }}) as valid_from_utc,
        {{ end_of_time() }} as valid_to_utc,
        true as is_current,
        
        -- For merge key
        order_message_id,
        
        -- Lineage
        _load_id
        
    from orders_with_events
)

select
    "Type",
    "ExtractDate",
    "Execution",
    "Unit",
    valid_from_utc,
    valid_to_utc,
    is_current,
    _load_id
from final
