{#
    Post-hook macro to close prior versions when corrections arrive.
    
    At order level: when a new version of an order arrives,
    close the previous version by setting valid_to and is_current = false.
    
    This runs after the incremental merge to handle cases where
    the same order appears in multiple loads.
#}

{% macro close_prior_order_versions() %}

{% set target_relation = this %}

{% if is_bigquery() %}

merge into {{ target_relation }} as target
using (
    select 
        older."Execution".OrderMessageID as order_id,
        older.valid_from_utc as older_valid_from,
        {{ timestamp_sub('newer.valid_from_utc', 1, 'SECOND') }} as new_valid_to
    from {{ target_relation }} older
    inner join {{ target_relation }} newer
        on older."Execution".OrderMessageID = newer."Execution".OrderMessageID
        and older.valid_from_utc < newer.valid_from_utc
    where older.is_current = true
      and newer.is_current = true
) as updates
on target."Execution".OrderMessageID = updates.order_id
   and target.valid_from_utc = updates.older_valid_from
   and target.is_current = true
when matched then update set
    valid_to_utc = updates.new_valid_to,
    is_current = false

{% elif is_duckdb() %}

update {{ target_relation }} as target
set 
    valid_to_utc = updates.new_valid_to,
    is_current = false
from (
    select 
        older."Execution"."OrderMessageID" as order_id,
        older.valid_from_utc as older_valid_from,
        newer.valid_from_utc - interval '1 second' as new_valid_to
    from {{ target_relation }} older
    inner join {{ target_relation }} newer
        on older."Execution"."OrderMessageID" = newer."Execution"."OrderMessageID"
        and older.valid_from_utc < newer.valid_from_utc
    where older.is_current = true
      and newer.is_current = true
) as updates
where target."Execution"."OrderMessageID" = updates.order_id
  and target.valid_from_utc = updates.older_valid_from
  and target.is_current = true

{% endif %}

{% endmacro %}
