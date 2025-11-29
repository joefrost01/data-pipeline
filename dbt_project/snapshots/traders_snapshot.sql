{% snapshot traders_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='trader_id',
        strategy='check',
        check_cols=['trader_name', 'desk_id', 'status', 'compliance_officer'],
        invalidate_hard_deletes=true
    )
}}

/*
    Trader dimension snapshot (SCD Type 2).
    
    Tracks historical changes for:
    - Trader desk moves (critical for surveillance attribution)
    - Status changes (active/inactive)
    - Compliance officer assignments
*/

select
    trader_id,
    trader_name,
    desk_id,
    status,
    compliance_officer
from {{ ref('stg_traders') }}

{% endsnapshot %}
