
        
            delete from "dev"."main_marts"."dim_trader"
            where (
                trader_sk) in (
                select (trader_sk)
                from "dim_trader__dbt_tmp20251213235814731061"
            );

        
    

    insert into "dev"."main_marts"."dim_trader" ("trader_sk", "trader_bk", "trader_id", "trader_name", "trader_type", "valid_from_utc", "valid_to_utc", "is_current")
    (
        select "trader_sk", "trader_bk", "trader_id", "trader_name", "trader_type", "valid_from_utc", "valid_to_utc", "is_current"
        from "dim_trader__dbt_tmp20251213235814731061"
    )
  