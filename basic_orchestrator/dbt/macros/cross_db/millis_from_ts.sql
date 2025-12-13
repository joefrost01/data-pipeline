{#
  Extract milliseconds component (0-999) from a timestamp.
  
  Usage:
    {{ millis_from_ts('event_timestamp_utc') }}
  
  Args:
    ts: Timestamp column or expression
  
  Returns:
    Integer 0-999 representing the milliseconds part
#}

{% macro millis_from_ts(ts) %}
  {% if target.type == 'bigquery' %}
    cast(mod(extract(millisecond from {{ ts }}), 1000) as int64)
  {% elif target.type == 'duckdb' %}
    cast(epoch_ms({{ ts }}) % 1000 as bigint)
  {% else %}
    cast(extract(millisecond from {{ ts }}) as bigint)
  {% endif %}
{% endmacro %}
