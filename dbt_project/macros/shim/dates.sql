{#
    Current timestamp.

    Named now_ts() to avoid collision with SQL's CURRENT_TIMESTAMP.

    BigQuery: CURRENT_TIMESTAMP()
    DuckDB:   current_timestamp
#}

{% macro now_ts() %}
    {% if is_duckdb() %}
        current_timestamp
    {% else %}
        current_timestamp()
    {% endif %}
{% endmacro %}


{#
    Current date.

    Named today_date() to avoid collision with SQL's CURRENT_DATE.
#}

{% macro today_date() %}
    {% if is_duckdb() %}
        current_date
    {% else %}
        current_date()
    {% endif %}
{% endmacro %}


{#
    Truncate timestamp to a given part.

    BigQuery: TIMESTAMP_TRUNC(ts, DAY)
    DuckDB:   DATE_TRUNC('day', ts)
#}

{% macro timestamp_trunc(timestamp_field, part) %}
    {% if is_duckdb() %}
        date_trunc('{{ part | lower }}', {{ timestamp_field }})
    {% else %}
        timestamp_trunc({{ timestamp_field }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Truncate date to a given part.

    BigQuery: DATE_TRUNC(dt, MONTH)
    DuckDB:   DATE_TRUNC('month', dt)
#}

{% macro date_trunc(date_field, part) %}
    {% if is_duckdb() %}
        date_trunc('{{ part | lower }}', {{ date_field }})
    {% else %}
        date_trunc({{ date_field }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Subtract days from a date.

    BigQuery: DATE_SUB(dt, INTERVAL n DAY)
    DuckDB:   dt - INTERVAL 'n days'
#}

{% macro date_sub_days(date_expr, days) %}
    {% if is_duckdb() %}
        {{ date_expr }} - interval '{{ days }} days'
    {% else %}
        date_sub({{ date_expr }}, interval {{ days }} day)
    {% endif %}
{% endmacro %}


{#
    Add days to a date.

    BigQuery: DATE_ADD(dt, INTERVAL n DAY)
    DuckDB:   dt + INTERVAL 'n days'
#}

{% macro date_add_days(date_expr, days) %}
    {% if is_duckdb() %}
        {{ date_expr }} + interval '{{ days }} days'
    {% else %}
        date_add({{ date_expr }}, interval {{ days }} day)
    {% endif %}
{% endmacro %}


{#
    Add interval to timestamp.

    BigQuery: TIMESTAMP_ADD(ts, INTERVAL 1 DAY)
    DuckDB:   ts + INTERVAL '1 day'
#}

{% macro timestamp_add(timestamp_field, interval_value, interval_unit) %}
    {% if is_duckdb() %}
        {{ timestamp_field }} + interval '{{ interval_value }} {{ interval_unit | lower }}'
    {% else %}
        timestamp_add({{ timestamp_field }}, interval {{ interval_value }} {{ interval_unit }})
    {% endif %}
{% endmacro %}


{#
    Subtract interval from timestamp.
#}

{% macro timestamp_sub(timestamp_field, interval_value, interval_unit) %}
    {% if is_duckdb() %}
        {{ timestamp_field }} - interval '{{ interval_value }} {{ interval_unit | lower }}'
    {% else %}
        timestamp_sub({{ timestamp_field }}, interval {{ interval_value }} {{ interval_unit }})
    {% endif %}
{% endmacro %}


{#
    Difference between two timestamps.

    BigQuery: TIMESTAMP_DIFF(ts1, ts2, SECOND)
    DuckDB:   DATEDIFF('second', ts2, ts1)  -- note argument order!
#}

{% macro timestamp_diff(timestamp1, timestamp2, part) %}
    {% if is_duckdb() %}
        datediff('{{ part | lower }}', {{ timestamp2 }}, {{ timestamp1 }})
    {% else %}
        timestamp_diff({{ timestamp1 }}, {{ timestamp2 }}, {{ part }})
    {% endif %}
{% endmacro %}


{#
    Parse string to timestamp.

    BigQuery: PARSE_TIMESTAMP('%Y-%m-%d', str)
    DuckDB:   strptime(str, '%Y-%m-%d')
#}

{% macro parse_timestamp(format_string, string_field) %}
    {% if is_duckdb() %}
        strptime({{ string_field }}, '{{ format_string }}')
    {% else %}
        parse_timestamp('{{ format_string }}', {{ string_field }})
    {% endif %}
{% endmacro %}


{#
    Format timestamp to string.

    BigQuery: FORMAT_TIMESTAMP('%Y-%m-%d', ts)
    DuckDB:   strftime(ts, '%Y-%m-%d')
#}

{% macro format_timestamp(format_string, timestamp_field) %}
    {% if is_duckdb() %}
        strftime({{ timestamp_field }}, '{{ format_string }}')
    {% else %}
        format_timestamp('{{ format_string }}', {{ timestamp_field }})
    {% endif %}
{% endmacro %}


{% macro date_spine(start_date, end_date, alias='date_spine', column='date_value') %}
    {% if is_duckdb() %}
        (select {{ column }}::date as {{ column }}
         from generate_series(
            {{ start_date }},
            {{ end_date }},
            interval '1 day'
         ) as t({{ column }})) as {{ alias }}
    {% else %}
        unnest(generate_date_array(
            {{ start_date }},
            {{ end_date }}
        )) as {{ alias }}({{ column }})
    {% endif %}
{% endmacro %}