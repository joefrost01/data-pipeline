{#
    Override dbt's default schema name generation.
    
    By default, dbt concatenates: <target_schema>_<custom_schema>
    This gives us names like "main_main" which we don't want for DuckDB.
    
    For DuckDB: Use 'main' schema - everything in one place
    For BigQuery: Use the custom schema directly (e.g., event_interaction)
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.type == 'duckdb' -%}
        {# DuckDB: always use main schema #}
        main
    {%- elif custom_schema_name is not none -%}
        {# BigQuery: use custom schema directly, not concatenated #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
