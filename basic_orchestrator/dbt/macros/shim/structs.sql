{#
    Struct shims for building nested data structures.
    
    BigQuery uses: STRUCT(value AS field_name, ...)
    DuckDB uses:   struct_pack(field_name := value, ...) or {'field_name': value, ...}
    
    We use struct_pack() for DuckDB as it handles nested structs better.
#}


{#
    Build a struct from field name/value pairs.
    
    Usage:
        {{ build_struct([
            ('Type', "'Master.Transaction'"),
            ('OrderID', 'order_id'),
            ('Amount', 'amount')
        ]) }}
    
    BigQuery output: STRUCT('Master.Transaction' AS Type, order_id AS OrderID, amount AS Amount)
    DuckDB output:   struct_pack(Type := 'Master.Transaction', OrderID := order_id, Amount := amount)
#}

{% macro build_struct(fields) %}
    {% if is_duckdb() %}
        struct_pack(
        {%- for name, value in fields -%}
            "{{ name }}" := {{ value }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {% else %}
        struct(
        {%- for name, value in fields -%}
            {{ value }} as {{ name }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {% endif %}
{% endmacro %}


{#
    Aggregate rows into an array of structs.
    
    Usage:
        {{ array_agg_struct([
            ('Type', 'event_type'),
            ('EventID', 'event_id'),
            ('Price', 'price')
        ], order_by='event_seq') }}
    
    BigQuery: ARRAY_AGG(STRUCT(...) ORDER BY event_seq)
    DuckDB:   list(struct_pack(...) ORDER BY event_seq)
#}

{% macro array_agg_struct(fields, order_by=none) %}
    {% if is_duckdb() %}
        list(
            struct_pack(
            {%- for name, value in fields -%}
                "{{ name }}" := {{ value }}{% if not loop.last %}, {% endif %}
            {%- endfor -%}
            )
            {% if order_by %}order by {{ order_by }}{% endif %}
        )
    {% else %}
        array_agg(
            struct(
            {%- for name, value in fields -%}
                {{ value }} as {{ name }}{% if not loop.last %}, {% endif %}
            {%- endfor -%}
            )
            {% if order_by %}order by {{ order_by }}{% endif %}
        )
    {% endif %}
{% endmacro %}


{#
    For complex nested structs where the simple macro doesn't work,
    use platform-specific SQL directly.
    
    Usage:
        {{ struct_sql(
            bq="STRUCT(a AS x, STRUCT(b AS y) AS nested)",
            duck="struct_pack(x := a, nested := struct_pack(y := b))"
        ) }}
#}

{% macro struct_sql(bq, duck) %}
    {% if is_duckdb() %}
        {{ duck }}
    {% else %}
        {{ bq }}
    {% endif %}
{% endmacro %}
