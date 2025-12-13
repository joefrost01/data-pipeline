{#
  SCD Type 2 dimension merge macro.
  
  This macro generates the SQL for maintaining a slowly-changing dimension
  with full history. It handles:
  - Inserting brand new business keys
  - Closing old records when attributes change (setting valid_to, is_current=false)
  - Inserting new versions for changed records
  
  The macro expects the target table to have these columns:
  - <business_key_column>: The natural/business key
  - <sk_column>: Surrogate key (computed from business key + valid_from)
  - valid_from_utc: When this version became effective
  - valid_to_utc: When this version was superseded (null or end_of_time for current)
  - is_current: Boolean flag for current version
  - Plus all attribute columns
  
  Usage in an incremental model:
  
  {{
    config(
      materialized='incremental',
      unique_key='<sk_column>',
      on_schema_change='append_new_columns'
    )
  }}
  
  {{ scd2_merge(
      source_cte='staged',
      business_key='org_bk',
      sk_column='org_sk',
      attribute_columns=['legal_entity', 'division', 'desk', 'book'],
      effective_ts_column='effective_ts'
  ) }}
  
  Args:
    source_cte: Name of the CTE containing new/changed records
    business_key: Column name of the business key
    sk_column: Column name for the surrogate key  
    attribute_columns: List of attribute column names to track for changes
    effective_ts_column: Column in source with the effective timestamp
#}

{% macro scd2_merge(source_cte, business_key, sk_column, attribute_columns, effective_ts_column='effective_ts') %}

{# Generate the attribute comparison expression #}
{% set attr_compare = [] %}
{% for col in attribute_columns %}
  {% do attr_compare.append("coalesce(cast(source." ~ col ~ " as varchar), '') != coalesce(cast(existing." ~ col ~ " as varchar), '')") %}
{% endfor %}

{% if not is_incremental() %}
-- First run: Insert all records as current
select
    source.{{ sk_column }},
    source.{{ business_key }},
    {% for col in attribute_columns %}
    source.{{ col }},
    {% endfor %}
    source.{{ effective_ts_column }} as valid_from_utc,
    {{ end_of_time() }} as valid_to_utc,
    true as is_current
from {{ source_cte }} as source

{% else %}
-- Incremental run: Insert new/changed records, close old versions

-- New records and updated versions
select
    source.{{ sk_column }},
    source.{{ business_key }},
    {% for col in attribute_columns %}
    source.{{ col }},
    {% endfor %}
    source.{{ effective_ts_column }} as valid_from_utc,
    {{ end_of_time() }} as valid_to_utc,
    true as is_current
from {{ source_cte }} as source
left join {{ this }} as existing
    on source.{{ business_key }} = existing.{{ business_key }}
    and existing.is_current = true
where
    -- New business key (no existing current record)
    existing.{{ business_key }} is null
    -- Or attributes have changed
    or ({{ attr_compare | join(' or ') }})

union all

-- Existing current records that need to be closed (attributes changed)
select
    existing.{{ sk_column }},
    existing.{{ business_key }},
    {% for col in attribute_columns %}
    existing.{{ col }},
    {% endfor %}
    existing.valid_from_utc,
    source.{{ effective_ts_column }} as valid_to_utc,  -- Close at the time of change
    false as is_current
from {{ this }} as existing
inner join {{ source_cte }} as source
    on source.{{ business_key }} = existing.{{ business_key }}
where
    existing.is_current = true
    and ({{ attr_compare | join(' or ') }})
{% endif %}

{% endmacro %}
