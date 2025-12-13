{#
  Safe cast that returns NULL on failure rather than erroring.
  
  Usage:
    {{ safe_cast('price_str', 'numeric') }}
    {{ safe_cast('qty_str', 'integer') }}
    {{ safe_cast('event_ts', 'timestamp') }}
  
  Args:
    column_name: Column or expression to cast
    target_type: Target data type
  
  Returns:
    Casted value or NULL if conversion fails
#}

{% macro safe_cast(column_name, target_type) %}
  {% if target.type == 'bigquery' %}
    safe_cast({{ column_name }} as {{ target_type }})
  {% elif target.type == 'duckdb' %}
    try_cast({{ column_name }} as {{ target_type }})
  {% else %}
    cast({{ column_name }} as {{ target_type }})
  {% endif %}
{% endmacro %}
