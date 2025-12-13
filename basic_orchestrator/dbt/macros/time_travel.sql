{#
  Time-travel filter macro for bi-temporal queries.
  
  Filters a bi-temporal table to show the state as of a specific timestamp.
  Returns records that were "current truth" at the given point in time.
  
  Usage:
    SELECT * 
    FROM {{ ref('fact_futures_order_event') }}
    WHERE {{ as_of('2025-12-12 09:15:00') }}
  
  Or with a column reference:
    WHERE {{ as_of('my_report_timestamp') }}
  
  Args:
    timestamp_expr: A timestamp literal ('2025-12-12 09:15:00') or column name
  
  Returns:
    SQL fragment: valid_from_utc <= <ts> AND valid_to_utc > <ts>
#}

{% macro as_of(timestamp_expr) %}
  valid_from_utc <= {{ timestamp_expr }} and valid_to_utc > {{ timestamp_expr }}
{% endmacro %}


{#
  Convenience macro for "what did we know at market open?"
  
  Usage:
    SELECT * 
    FROM {{ ref('fact_futures_order_event') }}
    WHERE {{ as_of_market_open('2025-12-12') }}
#}

{% macro as_of_market_open(date_expr, market_open_time='08:00:00') %}
  {% if target.type == 'bigquery' %}
    {{ as_of("timestamp(concat(" ~ date_expr ~ ", ' " ~ market_open_time ~ "'))") }}
  {% elif target.type == 'duckdb' %}
    {{ as_of("(" ~ date_expr ~ " || ' " ~ market_open_time ~ "')::timestamp") }}
  {% else %}
    {{ as_of("cast(" ~ date_expr ~ " || ' " ~ market_open_time ~ "' as timestamp)") }}
  {% endif %}
{% endmacro %}
