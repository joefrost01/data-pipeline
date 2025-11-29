{{
    config(
        materialized='incremental',
        unique_key='trade_id',
        incremental_strategy='merge',
        partition_by={
            'field': 'trade_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['trader_id', 'instrument_id']
    )
}}

/*
    Core curation model: enriched trades from all sources.
    
    This is where:
    - Deterministic trade_id is generated
    - Dimension lookups are applied (point-in-time for SCD-2)
    - All sources are unioned to canonical schema
*/

with murex as (
    select
        source_trade_id,
        source_system,
        trade_time,
        counterparty_name,
        trader_id,
        instrument_id,
        side,
        quantity,
        price,
        book_id,
        loaded_at
    from {{ ref('stg_murex_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

venue_a as (
    select
        source_trade_id,
        source_system,
        trade_time,
        counterparty_name,
        trader_id,
        instrument_id,
        side,
        quantity,
        price,
        book_id,
        loaded_at
    from {{ ref('stg_venue_a_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
),

all_trades as (
    select * from murex
    union all
    select * from venue_a
),

enriched as (
    select
        -- Deterministic trade identity
        {{ generate_trade_id("'markets'", "t.source_system", "t.source_trade_id") }} as trade_id,
        
        -- Source lineage
        t.source_trade_id,
        t.source_system,
        
        -- Trade attributes
        t.trade_time,
        date(t.trade_time) as trade_date,
        t.side,
        t.quantity,
        t.price,
        t.quantity * t.price as notional,
        
        -- Trader (point-in-time lookup for SCD-2)
        t.trader_id,
        tr.trader_name,
        tr.desk_id,
        tr.compliance_officer,
        
        -- Counterparty
        t.counterparty_name,
        cp.counterparty_id,
        cp.lei as counterparty_lei,
        
        -- Instrument (current state, SCD-1)
        t.instrument_id,
        inst.symbol as instrument_symbol,
        inst.isin,
        inst.asset_class,
        inst.currency,
        
        -- Book
        coalesce(t.book_id, tr.desk_id) as book_id,  -- Fall back to trader's desk
        bk.book_name,
        bk.legal_entity,
        
        -- Audit
        t.loaded_at,
        current_timestamp() as enriched_at
        
    from all_trades t
    
    -- Trader lookup (point-in-time for SCD-2)
    left join {{ ref('traders_snapshot') }} tr
        on t.trader_id = tr.trader_id
        and t.trade_time >= tr.dbt_valid_from
        and (t.trade_time < tr.dbt_valid_to or tr.dbt_valid_to is null)
    
    -- Counterparty lookup (point-in-time)
    left join {{ ref('counterparties_snapshot') }} cp
        on t.counterparty_name = cp.counterparty_name
        and t.trade_time >= cp.dbt_valid_from
        and (t.trade_time < cp.dbt_valid_to or cp.dbt_valid_to is null)
    
    -- Instrument lookup (current state only)
    left join {{ ref('dim_instrument') }} inst
        on t.instrument_id = inst.instrument_id
    
    -- Book lookup (point-in-time)
    left join {{ ref('books_snapshot') }} bk
        on coalesce(t.book_id, tr.desk_id) = bk.book_id
        and t.trade_time >= bk.dbt_valid_from
        and (t.trade_time < bk.dbt_valid_to or bk.dbt_valid_to is null)
)

select * from enriched
