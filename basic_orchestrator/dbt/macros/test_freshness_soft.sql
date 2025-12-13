{% test freshness_soft(model, column, max_hours=24) %}
  {#
    Soft freshness test - warns but doesn't fail.
    Use for "nice to have" freshness requirements.
  #}
  select 1
  where (
    select max({{ column }})
    from {{ model }}
  ) < {{ dbt.dateadd('hour', -max_hours, dbt.current_timestamp()) }}
{% endtest %}