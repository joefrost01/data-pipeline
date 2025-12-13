{#
  Surrogate key generation macro.
  
  Generates a deterministic BIGINT surrogate key from one or more business key fields.
  Uses different hash functions for DuckDB vs BigQuery but produces consistent
  results within each platform.
  
  Usage:
    {{ sk(['field1', 'field2']) }}
    {{ sk(['order_id']) }}
  
  Args:
    fields: List of column names to hash together
  
  Returns:
    BIGINT surrogate key
#}

{% macro sk(fields) %}
  {% if target.type == 'bigquery' %}
    {# BigQuery: MD5 hash truncated to 64 bits, cast to INT64 #}
    cast(
      concat('0x', substr(to_hex(md5(concat({{ fields | join(", '|', ") }}))), 1, 16))
      as int64
    )
  {% elif target.type == 'duckdb' %}
    {# DuckDB: Native hash function, masked to fit in signed BIGINT range #}
    (hash(concat_ws('|', {{ fields | join(", ") }})) & 9223372036854775807)::bigint
  {% else %}
    {# Fallback for other adapters #}
    {{ dbt_utils.generate_surrogate_key(fields) }}
  {% endif %}
{% endmacro %}
