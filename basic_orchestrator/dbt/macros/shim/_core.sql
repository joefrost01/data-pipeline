{#
    Core shim utilities.

    These are the building blocks used by other shim macros.
#}

{% macro is_duckdb() %}
    {{ return(target.type == 'duckdb') }}
{% endmacro %}


{% macro is_bigquery() %}
    {{ return(target.type == 'bigquery') }}
{% endmacro %}


{#
    Map BigQuery types to DuckDB equivalents.
    Models use BigQuery type names; we translate for DuckDB.

    Unknown types fall back to VARCHAR for safety.
#}

{% macro map_type(bq_type) %}
    {% set type_map = {
        'INT64': 'BIGINT',
        'FLOAT64': 'DOUBLE',
        'NUMERIC': 'DECIMAL(38, 9)',
        'BIGNUMERIC': 'DECIMAL(76, 38)',
        'BOOL': 'BOOLEAN',
        'STRING': 'VARCHAR',
        'BYTES': 'BLOB',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'DATETIME': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMPTZ',
        'GEOGRAPHY': 'VARCHAR',
        'JSON': 'JSON',
    } %}

    {% set bq_type_upper = bq_type | upper %}

    {% if is_duckdb() %}
        {# Return mapped type, or VARCHAR as safe fallback for unknown types #}
        {{ return(type_map.get(bq_type_upper, 'VARCHAR')) }}
    {% else %}
        {{ return(bq_type) }}
    {% endif %}
{% endmacro %}
