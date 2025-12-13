
        
            delete from "dev"."main_marts"."dim_org"
            where (
                org_sk) in (
                select (org_sk)
                from "dim_org__dbt_tmp20251213235814715244"
            );

        
    

    insert into "dev"."main_marts"."dim_org" ("org_sk", "org_bk", "legal_entity", "division", "desk", "book", "valid_from_utc", "valid_to_utc", "is_current")
    (
        select "org_sk", "org_bk", "legal_entity", "division", "desk", "book", "valid_from_utc", "valid_to_utc", "is_current"
        from "dim_org__dbt_tmp20251213235814715244"
    )
  