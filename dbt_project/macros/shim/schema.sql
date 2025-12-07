{#
    Combined partitioning and clustering config for model config blocks.

    Partitioning and clustering are BigQuery-specific optimizations.
    On DuckDB, we just return empty config (the tables still work).

    Usage:
        {{ config(
            materialized='incremental',
            **table_config(
                partition_field='trade_date',
                cluster_fields=['source_system', 'instrument_id']
            )
        ) }}
#}

{% macro table_config(partition_field=none, cluster_fields=none) %}
    {% set config = {} %}

    {% if is_bigquery() %}
        {% if partition_field %}
            {% do config.update({'partition_by': {'field': partition_field, 'data_type': 'date'}}) %}
        {% endif %}
        {% if cluster_fields %}
            {% do config.update({'cluster_by': cluster_fields}) %}
        {% endif %}
    {% endif %}

    {{ return(config) }}
{% endmacro %}