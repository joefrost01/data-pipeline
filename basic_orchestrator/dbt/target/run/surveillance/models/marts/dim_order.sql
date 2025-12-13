
        
            delete from "dev"."main_marts"."dim_order"
            where (
                order_sk) in (
                select (order_sk)
                from "dim_order__dbt_tmp20251213235814603878"
            );

        
    

    insert into "dev"."main_marts"."dim_order" ("order_sk", "order_id", "parent_order_id", "client_order_id", "broker_order_id", "exchange_order_id", "side", "order_type", "time_in_force", "quantity", "limit_price", "stop_price", "strategy_id", "source_system")
    (
        select "order_sk", "order_id", "parent_order_id", "client_order_id", "broker_order_id", "exchange_order_id", "side", "order_type", "time_in_force", "quantity", "limit_price", "stop_price", "strategy_id", "source_system"
        from "dim_order__dbt_tmp20251213235814603878"
    )
  