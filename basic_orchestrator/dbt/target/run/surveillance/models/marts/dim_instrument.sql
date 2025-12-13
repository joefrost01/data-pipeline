
        
            delete from "dev"."main_marts"."dim_instrument"
            where (
                instrument_sk) in (
                select (instrument_sk)
                from "dim_instrument__dbt_tmp20251213235814646067"
            );

        
    

    insert into "dev"."main_marts"."dim_instrument" ("instrument_sk", "instrument_bk", "instrument_id", "symbol", "exchange", "product_type", "contract_month", "currency", "multiplier", "tick_size", "valid_from_utc", "valid_to_utc", "is_current")
    (
        select "instrument_sk", "instrument_bk", "instrument_id", "symbol", "exchange", "product_type", "contract_month", "currency", "multiplier", "tick_size", "valid_from_utc", "valid_to_utc", "is_current"
        from "dim_instrument__dbt_tmp20251213235814646067"
    )
  