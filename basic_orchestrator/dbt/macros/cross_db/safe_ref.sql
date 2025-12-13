{% macro safe_ref(model_name) %}
  {#
    Reference a model, but return empty result set if it doesn't exist.
    Useful for optional dependencies.
  #}
  {% set relation = adapter.get_relation(
    database=target.database,
    schema=target.schema,
    identifier=model_name
  ) %}

  {% if relation %}
    {{ return(ref(model_name)) }}
  {% else %}
    {{ return(relation) }}
  {% endif %}
{% endmacro %}