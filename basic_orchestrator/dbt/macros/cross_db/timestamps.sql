{#
  Get current UTC timestamp in a cross-platform way.
  
  Usage:
    {{ current_ts() }}
  
  Returns:
    Current timestamp in UTC
#}

{% macro current_ts() %}
  {% if target.type == 'bigquery' %}
    current_timestamp()
  {% elif target.type == 'duckdb' %}
    current_timestamp
  {% else %}
    current_timestamp
  {% endif %}
{% endmacro %}


{#
  Get a "far future" timestamp for SCD2 valid_to on current records.
  Using 9999-12-31 as the standard end-of-time sentinel.
  
  Usage:
    {{ end_of_time() }}
  
  Returns:
    Timestamp representing "forever" for SCD2 valid_to
#}

{% macro end_of_time() %}
  {% if target.type == 'bigquery' %}
    timestamp('9999-12-31 23:59:59')
  {% elif target.type == 'duckdb' %}
    '9999-12-31 23:59:59'::timestamp
  {% else %}
    cast('9999-12-31 23:59:59' as timestamp)
  {% endif %}
{% endmacro %}
