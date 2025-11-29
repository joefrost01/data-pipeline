{% macro generate_trade_id(domain, source_system, source_trade_id) %}
{#
    Generates a deterministic UUID5 for trade identity.
    
    Same inputs always produce same output, enabling:
    - Idempotent reprocessing
    - Reliable cross-system joins
    - No coordination between parallel processes
    
    Args:
        domain: Business domain (e.g., 'markets', 'treasury')
        source_system: Upstream system code (e.g., 'MUREX', 'VENUE_A')
        source_trade_id: Native identifier from source
    
    Returns:
        Deterministic UUID as STRING
#}
to_hex(
    md5(
        concat(
            '{{ var("markets_namespace") }}',
            ':',
            {{ domain }},
            ':',
            {{ source_system }},
            ':',
            cast({{ source_trade_id }} as string)
        )
    )
)
{% endmacro %}
