{#
    Safe casting - returns NULL on failure instead of erroring.

    BigQuery: SAFE_CAST(x AS type)
    DuckDB:   TRY_CAST(x AS type)
#}

{% macro safe_cast(field, target_type) %}
    {% if is_duckdb() %}
        try_cast({{ field }} as {{ map_type(target_type) }})
    {% else %}
        safe_cast({{ field }} as {{ target_type }})
    {% endif %}
{% endmacro %}


{#
    Standard cast (errors on failure).
#}

{% macro typed_cast(field, target_type) %}
    cast({{ field }} as {{ map_type(target_type) }})
{% endmacro %}
