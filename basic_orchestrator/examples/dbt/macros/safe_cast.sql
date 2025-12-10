{% macro safe_cast(column_name, target_type) %}
  {#
    Cast with null on failure rather than error.
    Works for BigQuery and DuckDB.
  #}
  {% if target.type == 'bigquery' %}
    safe_cast({{ column_name }} as {{ target_type }})
  {% elif target.type == 'duckdb' %}
    try_cast({{ column_name }} as {{ target_type }})
  {% else %}
    cast({{ column_name }} as {{ target_type }})
  {% endif %}
{% endmacro %}