{#
    Safe divide - returns NULL instead of error on divide by zero.

    BigQuery: SAFE_DIVIDE(a, b)
    DuckDB:   a / NULLIF(b, 0)
#}

{% macro safe_divide(numerator, denominator) %}
    {% if is_duckdb() %}
        {{ numerator }} / nullif({{ denominator }}, 0)
    {% else %}
        safe_divide({{ numerator }}, {{ denominator }})
    {% endif %}
{% endmacro %}


{#
    Regexp contains.

    BigQuery: REGEXP_CONTAINS(str, r'pattern')
    DuckDB:   REGEXP_MATCHES(str, 'pattern')
#}

{% macro regexp_contains(string_field, pattern) %}
    {% if is_duckdb() %}
        regexp_matches({{ string_field }}, '{{ pattern }}')
    {% else %}
        regexp_contains({{ string_field }}, r'{{ pattern }}')
    {% endif %}
{% endmacro %}


{#
    Regexp extract.

    BigQuery: REGEXP_EXTRACT(str, r'pattern')
    DuckDB:   REGEXP_EXTRACT(str, 'pattern')
#}

{% macro regexp_extract(string_field, pattern) %}
    {% if is_duckdb() %}
        regexp_extract({{ string_field }}, '{{ pattern }}')
    {% else %}
        regexp_extract({{ string_field }}, r'{{ pattern }}')
    {% endif %}
{% endmacro %}