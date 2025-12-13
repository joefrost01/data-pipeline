{% macro log_run_results() %}
  {#
    Emit structured JSON for each model result.
    Dynatrace can parse these for alerting.
  #}
  {% if execute %}
    {% for result in results %}
      {{ log(tojson({
        "event": "dbt_model_result",
        "model": result.node.name,
        "status": result.status,
        "execution_time": result.execution_time,
        "rows_affected": result.adapter_response.rows_affected if result.adapter_response else none,
        "message": result.message,
        "timestamp": modules.datetime.datetime.utcnow().isoformat()
      }), info=true) }}
    {% endfor %}
  {% endif %}
{% endmacro %}