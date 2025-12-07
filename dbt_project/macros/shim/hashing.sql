{#
    MD5 hash returning hex string.

    BigQuery: TO_HEX(MD5(str))
    DuckDB:   MD5(str)  -- already returns hex
#}

{% macro md5_hex(value) %}
    {% if is_duckdb() %}
        md5({{ value }})
    {% else %}
        to_hex(md5({{ value }}))
    {% endif %}
{% endmacro %}


{#
    SHA256 hash returning hex string.

    BigQuery: TO_HEX(SHA256(str))
    DuckDB:   SHA256(str)  -- returns hex
#}

{% macro sha256_hex(value) %}
    {% if is_duckdb() %}
        sha256({{ value }})
    {% else %}
        to_hex(sha256({{ value }}))
    {% endif %}
{% endmacro %}


{#
    Generate a random UUID.

    Named gen_uuid() to avoid collision with BigQuery's GENERATE_UUID().

    BigQuery: GENERATE_UUID()
    DuckDB:   uuid()
#}

{% macro gen_uuid() %}
    {% if is_duckdb() %}
        uuid()
    {% else %}
        generate_uuid()
    {% endif %}
{% endmacro %}