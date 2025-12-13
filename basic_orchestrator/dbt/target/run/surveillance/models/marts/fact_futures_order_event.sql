
        
            delete from "dev"."main_marts"."fact_futures_order_event"
            where (
                fact_event_sk) in (
                select (fact_event_sk)
                from "fact_futures_order_event__dbt_tmp20251213235814765988"
            );

        
    

    insert into "dev"."main_marts"."fact_futures_order_event" ("fact_event_sk", "event_bk", "valid_from_utc", "valid_to_utc", "is_current", "business_date", "order_sk", "event_seq", "date_key", "time_key", "event_millis", "event_timestamp_utc", "org_sk", "trader_sk", "account_sk", "instrument_sk", "extra_sk", "event_type", "order_status", "filled_quantity", "remaining_quantity", "last_fill_qty", "last_fill_price", "avg_fill_price", "best_bid", "best_ask", "mid_price", "spread", "pre_trade_risk_check", "risk_limit_id", "current_position", "max_position_limit", "reject_reason", "_load_id", "feed_name")
    (
        select "fact_event_sk", "event_bk", "valid_from_utc", "valid_to_utc", "is_current", "business_date", "order_sk", "event_seq", "date_key", "time_key", "event_millis", "event_timestamp_utc", "org_sk", "trader_sk", "account_sk", "instrument_sk", "extra_sk", "event_type", "order_status", "filled_quantity", "remaining_quantity", "last_fill_qty", "last_fill_price", "avg_fill_price", "best_bid", "best_ask", "mid_price", "spread", "pre_trade_risk_check", "risk_limit_id", "current_position", "max_position_limit", "reject_reason", "_load_id", "feed_name"
        from "fact_futures_order_event__dbt_tmp20251213235814765988"
    )
  