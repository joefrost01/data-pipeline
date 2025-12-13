{{
  config(
    materialized='incremental',
    unique_key='extra_sk'
  )
}}

{#
  Sparse dimension for schema drift.
  
  Only stores distinct _extra JSON values. Fact table gets FK (extra_sk).
  Many fact rows can point to one dim_extra row if they have identical _extra.
  
  Includes a sentinel row (extra_sk = -1) for "no schema drift" so fact
  can always join without NULLs.

  Query pattern:
    SELECT f.*, e._extra
    FROM fact_futures_order_event f
    JOIN dim_extra e ON f.extra_sk = e.extra_sk
    WHERE e.extra_sk != -1  -- Exclude "no drift" rows if needed
#}

{% if not is_incremental() %}
-- Sentinel row for "no schema drift"
select
    cast(-1 as bigint) as extra_sk,
    cast(null as varchar) as _extra

union all
{% endif %}

select
    {{ sk(['_extra']) }} as extra_sk,
    _extra
from (
    select distinct _extra
    from {{ ref('stg_futures_order_events') }}
    where _extra is not null

    {% if is_incremental() %}
    and _extra not in (select _extra from {{ this }} where extra_sk != -1)
    {% endif %}
) new_extras