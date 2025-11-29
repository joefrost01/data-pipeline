{% macro generate_trade_id(domain, source_system, source_trade_id) %}
{#
    Generates a deterministic identifier for trade identity.
    
    Uses MD5 hash of a namespaced identity string to produce a 32-character
    hex identifier. This is NOT a UUID5 (which uses SHA-1 with specific 
    formatting) but provides the same deterministic properties:
    
    - Same inputs always produce same output
    - Enables idempotent reprocessing
    - Reliable cross-system joins
    - No coordination between parallel processes
    
    The format is: MD5(namespace:domain:source_system:source_trade_id)
    
    Args:
        domain: Business domain (e.g., 'markets', 'treasury')
        source_system: Upstream system code (e.g., 'MUREX', 'VENUE_A')
        source_trade_id: Native identifier from source
    
    Returns:
        Deterministic 32-character hex string
    
    Example:
        {{ generate_trade_id("'markets'", "source_system", "source_trade_id") }}
        
        With inputs ('markets', 'MUREX', 'TRD-12345') produces:
        '7a3f2b1c9d8e4f5a6b7c8d9e0f1a2b3c' (consistent across runs)
    
    Note on namespace:
        The namespace UUID is defined in dbt_project.yml as 'markets_namespace'.
        Changing the namespace will change ALL generated trade_ids, breaking:
        - Incremental model unique keys
        - Historical joins
        - Partner reconciliation
        
        The namespace is protected by CI validation.
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


{% macro generate_event_id(domain, source_system, source_event_id) %}
{#
    Generates a deterministic identifier for regulatory events.
    
    Uses the same approach as generate_trade_id for consistency.
    
    Args:
        domain: Business domain
        source_system: Upstream system code
        source_event_id: Native event identifier from source
    
    Returns:
        Deterministic 32-character hex string
#}
to_hex(
    md5(
        concat(
            '{{ var("markets_namespace") }}',
            ':event:',
            {{ domain }},
            ':',
            {{ source_system }},
            ':',
            cast({{ source_event_id }} as string)
        )
    )
)
{% endmacro %}
