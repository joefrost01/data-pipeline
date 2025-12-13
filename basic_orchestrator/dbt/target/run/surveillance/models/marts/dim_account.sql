
        
            delete from "dev"."main_marts"."dim_account"
            where (
                account_sk) in (
                select (account_sk)
                from "dim_account__dbt_tmp20251213235814643449"
            );

        
    

    insert into "dev"."main_marts"."dim_account" ("account_sk", "account_bk", "account_id", "valid_from_utc", "valid_to_utc", "is_current")
    (
        select "account_sk", "account_bk", "account_id", "valid_from_utc", "valid_to_utc", "is_current"
        from "dim_account__dbt_tmp20251213235814643449"
    )
  