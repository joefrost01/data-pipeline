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
    counterparty_id,
    counterparty_name,
    lei,
    country,
    status
from {{ ref('stg_counterparties') }}

{% endsnapshot %}
