{#
  Extract integer time key (HHMMSS) from a timestamp.
  
  Usage:
    {{ time_key_from_ts('event_timestamp_utc') }}
  
  Args:
    ts: Timestamp column or expression
  
  Returns:
    Integer in HHMMSS format (e.g., 143052 for 14:30:52)
#}

{% macro time_key_from_ts(ts) %}
  {% if target.type == 'bigquery' %}
    cast(format_timestamp('%H%M%S', {{ ts }}) as int64)
  {% elif target.type == 'duckdb' %}
    cast(strftime({{ ts }}, '%H%M%S') as bigint)
  {% else %}
    cast(to_char({{ ts }}, 'HH24MISS') as bigint)
  {% endif %}
{% endmacro %}
