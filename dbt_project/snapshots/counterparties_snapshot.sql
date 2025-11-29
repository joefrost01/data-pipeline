{% snapshot counterparties_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='counterparty_id',
        strategy='check',
        check_cols=['counterparty_name', 'lei', 'country', 'status'],
        invalidate_hard_deletes=true
    )
}}

/*
    Counterparty dimension snapshot (SCD Type 2).
    
    Tracks historical changes for:
    - Legal entity name changes
    - LEI updates
    - Jurisdiction changes
*/

select
    cast(counterparty_id as string) as counterparty_id,
    cast(counterparty_name as string) as counterparty_name,
    cast(lei as string) as lei,
    cast(country as string) as country,
    cast(status as string) as status
from {{ source('reference', 'counterparties') }}
where counterparty_id is not null

{% endsnapshot %}
