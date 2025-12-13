{#
  Extract integer date key (YYYYMMDD) from a timestamp.
  
  Usage:
    {{ date_key_from_ts('event_timestamp_utc') }}
  
  Args:
    ts: Timestamp column or expression
  
  Returns:
    Integer in YYYYMMDD format (e.g., 20240115)
#}

{% macro date_key_from_ts(ts) %}
  {% if target.type == 'bigquery' %}
    cast(format_timestamp('%Y%m%d', {{ ts }}) as int64)
  {% elif target.type == 'duckdb' %}
    cast(strftime({{ ts }}, '%Y%m%d') as bigint)
  {% else %}
    cast(to_char({{ ts }}, 'YYYYMMDD') as bigint)
  {% endif %}
{% endmacro %}
