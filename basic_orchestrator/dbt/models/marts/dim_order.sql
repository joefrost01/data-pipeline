{{
  config(
    materialized='incremental',
    unique_key='order_sk',
    on_schema_change='append_new_columns'
  )
}}

{#
  Order dimension.
  
  Business key: order_id
  
  Type 1 dimension (no history) - order static attributes are captured
  at order creation time and don't change.
  
  Note: This is NOT SCD2 because order parameters (side, order_type,
  quantity etc) are immutable. What changes is order STATUS, which
  is captured in the fact table as event_type/order_status.
#}

with events as (
    select * from {{ ref('stg_futures_order_events') }}
),

-- Get order attributes from the NEW event (first event for each order)
order_creation as (
    select
        order_id,
        parent_order_id,
        client_order_id,
        broker_order_id,
        exchange_order_id,
        side,
        order_type,
        time_in_force,
        quantity,
        limit_price,
        stop_price,
        strategy_id,
        source_system,
        event_timestamp_utc as created_at_utc,
        row_number() over (partition by order_id order by event_seq) as rn
    from events
    where event_type = 'NEW'
),

new_orders as (
    select
        order_id,
        parent_order_id,
        client_order_id,
        broker_order_id,
        exchange_order_id,
        side,
        order_type,
        time_in_force,
        quantity,
        limit_price,
        stop_price,
        strategy_id,
        source_system,
        created_at_utc
    from order_creation
    where rn = 1
),

final as (
    select
        -- Surrogate key
        {{ sk(['order_id']) }} as order_sk,
        
        -- Business key and identifiers
        order_id,
        parent_order_id,
        client_order_id,
        broker_order_id,
        exchange_order_id,
        
        -- Order parameters (immutable)
        side,
        order_type,
        time_in_force,
        quantity,
        limit_price,
        stop_price,
        
        -- Routing
        strategy_id,
        source_system
        
    from new_orders
    
    {% if is_incremental() %}
    -- Only insert orders we haven't seen before
    where order_id not in (select order_id from {{ this }})
    {% endif %}
)

select * from final
