{% macro normalise_counterparty_name(col) %}
    -- Simple local implementation for DuckDB dev.
    -- Normalises a counterparty name to a canonical form.
    lower(trim({{ col }}))
{% endmacro %}
