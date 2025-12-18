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


{#
    Regexp replace.

    BigQuery: REGEXP_REPLACE(str, r'pattern', replacement)
    DuckDB:   REGEXP_REPLACE(str, 'pattern', replacement)
#}

{% macro regexp_replace(string_field, pattern, replacement) %}
    {% if is_duckdb() %}
        regexp_replace({{ string_field }}, '{{ pattern }}', '{{ replacement }}')
    {% else %}
        regexp_replace({{ string_field }}, r'{{ pattern }}', '{{ replacement }}')
    {% endif %}
{% endmacro %}


{#
    String concatenation with separator.

    BigQuery: CONCAT(a, ':', b)
    DuckDB:   CONCAT(a, ':', b)  -- same
#}

{% macro concat_ws(separator, fields) %}
    concat_ws('{{ separator }}', {{ fields | join(', ') }})
{% endmacro %}
