



with events as (
    select * from "dev"."main_staging"."stg_futures_order_events"
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
        
  
    
    (hash(concat_ws('|', order_id)) & 9223372036854775807)::bigint
  
 as order_sk,
        
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
    
    
    -- Only insert orders we haven't seen before
    where order_id not in (select order_id from "dev"."main_marts"."dim_order")
    
)

select * from final