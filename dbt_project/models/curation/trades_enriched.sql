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
        cluster_by=['trader_id', 'instrument_id'],
        
        -- Full refresh weekly to pick up late reference data changes
        -- This is a trade-off: we prioritise processing speed over 
        -- perfect enrichment. Trades within the 7-day window will be
        -- re-enriched on the weekly full refresh.
        full_refresh = var('force_full_refresh', false)
    )
}}

/*
    Core curation model: enriched trades from all sources.
    
    This is where:
    - Deterministic trade_id is generated
    - Dimension lookups are applied (point-in-time for SCD-2)
    - All sources are unioned to canonical schema
    
    ## Reference Data Handling
    
    This model uses INCREMENTAL processing for efficiency. This means:
    
    1. Reference data changes AFTER a trade is processed won't be reflected
       until the next full refresh.
    
    2. For SCD-2 dimensions (traders, counterparties, books), we use 
       point-in-time lookups based on trade_time.
    
    3. For SCD-1 dimensions (instruments), we use current state.
    
    To force a full refresh (e.g., after major reference data corrections):
        dbt run --select trades_enriched --vars '{"force_full_refresh": true}'
    
    ## Counterparty Matching
    
    Some sources provide counterparty_id, others only provide counterparty_name.
    We handle both:
    
    1. If counterparty_id is provided, join on ID (preferred)
    2. If only name is provided, join on normalised name
    3. Normalisation: UPPER, trimmed, common suffixes removed
    
    This is imperfect but pragmatic. Manual mapping overrides can be added
    to the dim_counterparty_alias table for known mismatches.
*/

-- Normalise counterparty names for fuzzy matching
{% macro normalise_counterparty_name(name_column) %}
    regexp_replace(
        regexp_replace(
            upper(trim({{ name_column }})),
            r'\s+(LTD|LIMITED|PLC|INC|LLC|CORP|CORPORATION|AG|GMBH|SA|NV|BV)\.?$',
            ''
        ),
        r'\s+',
        ' '
    )
{% endmacro %}

with murex as (
    select
        source_trade_id,
        source_system,
        trade_time,
        counterparty_name,
        cast(null as string) as counterparty_id,  -- Murex provides name only
        trader_id,
        instrument_id,
        side,
        quantity,
        price,
        book_id,
        loaded_at
    from {{ ref('stg_murex_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
),

venue_a as (
    select
        source_trade_id,
        source_system,
        trade_time,
        counterparty_name,
        cast(null as string) as counterparty_id,
        trader_id,
        instrument_id,
        -- Normalise side values from Venue A (accepts B/S)
        case 
            when upper(side) in ('B', 'BUY') then 'BUY'
            when upper(side) in ('S', 'SELL') then 'SELL'
            else upper(side)
        end as side,
        quantity,
        price,
        book_id,
        loaded_at
    from {{ ref('stg_venue_a_trades') }}
    {% if is_incremental() %}
    where loaded_at > (select coalesce(max(loaded_at), '1900-01-01') from {{ this }})
    {% endif %}
),

all_trades as (
    select * from murex
    union all
    select * from venue_a
),

-- Build counterparty lookup with both ID and normalised name keys
counterparty_lookup as (
    select
        counterparty_id,
        counterparty_name,
        {{ normalise_counterparty_name('counterparty_name') }} as normalised_name,
        lei as counterparty_lei,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('counterparties_snapshot') }}
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
        
        -- Counterparty (prefer ID match, fall back to normalised name)
        coalesce(t.counterparty_id, cp_by_id.counterparty_id, cp_by_name.counterparty_id) as counterparty_id,
        coalesce(t.counterparty_name, cp_by_id.counterparty_name, cp_by_name.counterparty_name) as counterparty_name,
        coalesce(cp_by_id.counterparty_lei, cp_by_name.counterparty_lei) as counterparty_lei,
        
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
        
        -- Enrichment quality flags (useful for monitoring)
        case
            when tr.trader_id is null then true
            else false
        end as trader_not_found,
        case
            when cp_by_id.counterparty_id is null and cp_by_name.counterparty_id is null then true
            else false
        end as counterparty_not_found,
        case
            when inst.instrument_id is null then true
            else false
        end as instrument_not_found,
        
        -- Audit
        t.loaded_at,
        current_timestamp() as enriched_at
        
    from all_trades t
    
    -- Trader lookup (point-in-time for SCD-2)
    left join {{ ref('traders_snapshot') }} tr
        on t.trader_id = tr.trader_id
        and t.trade_time >= tr.dbt_valid_from
        and (t.trade_time < tr.dbt_valid_to or tr.dbt_valid_to is null)
    
    -- Counterparty lookup by ID (if provided)
    left join counterparty_lookup cp_by_id
        on t.counterparty_id is not null
        and t.counterparty_id = cp_by_id.counterparty_id
        and t.trade_time >= cp_by_id.dbt_valid_from
        and (t.trade_time < cp_by_id.dbt_valid_to or cp_by_id.dbt_valid_to is null)
    
    -- Counterparty lookup by normalised name (fallback)
    left join counterparty_lookup cp_by_name
        on t.counterparty_id is null
        and t.counterparty_name is not null
        and {{ normalise_counterparty_name('t.counterparty_name') }} = cp_by_name.normalised_name
        and t.trade_time >= cp_by_name.dbt_valid_from
        and (t.trade_time < cp_by_name.dbt_valid_to or cp_by_name.dbt_valid_to is null)
    
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
