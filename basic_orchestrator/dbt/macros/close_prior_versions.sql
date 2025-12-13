{#
  Post-hook macro to close prior versions when corrections arrive.
  
  Logic:
  1. For each (feed_name, business_date) combination in the fact table
  2. Find rows where valid_to_utc = 9999-12-31 (currently "current")
  3. If multiple such rows exist for the same event_bk, close the older ones
  
  This runs AFTER the INSERT, so new rows are already in the table.
  We update older rows to set valid_to = newer row's valid_from - 1 second.
#}

{% macro close_prior_versions(fact_table) %}

{% if target.type == 'bigquery' %}
-- BigQuery: Use MERGE to update
MERGE INTO {{ fact_table }} AS target
USING (
    -- Find rows that should be closed: same event_bk but older valid_from
    SELECT 
        older.fact_event_sk,
        TIMESTAMP_SUB(newer.valid_from_utc, INTERVAL 1 SECOND) AS new_valid_to
    FROM {{ fact_table }} older
    INNER JOIN {{ fact_table }} newer
        ON older.event_bk = newer.event_bk
        AND older.valid_from_utc < newer.valid_from_utc
    WHERE older.valid_to_utc = TIMESTAMP('9999-12-31 23:59:59')
      AND older.is_current = true
) AS updates
ON target.fact_event_sk = updates.fact_event_sk
WHEN MATCHED THEN UPDATE SET
    valid_to_utc = updates.new_valid_to,
    is_current = false

{% elif target.type == 'duckdb' %}
-- DuckDB: Use UPDATE with subquery
UPDATE {{ fact_table }} AS target
SET 
    valid_to_utc = updates.new_valid_to,
    is_current = false
FROM (
    SELECT 
        older.fact_event_sk,
        newer.valid_from_utc - INTERVAL '1 second' AS new_valid_to
    FROM {{ fact_table }} older
    INNER JOIN {{ fact_table }} newer
        ON older.event_bk = newer.event_bk
        AND older.valid_from_utc < newer.valid_from_utc
    WHERE older.valid_to_utc = TIMESTAMP '9999-12-31 23:59:59'
      AND older.is_current = true
) AS updates
WHERE target.fact_event_sk = updates.fact_event_sk

{% else %}
-- Generic SQL: Standard UPDATE
UPDATE {{ fact_table }}
SET 
    valid_to_utc = (
        SELECT MIN(newer.valid_from_utc) - INTERVAL '1 second'
        FROM {{ fact_table }} newer
        WHERE newer.event_bk = {{ fact_table }}.event_bk
          AND newer.valid_from_utc > {{ fact_table }}.valid_from_utc
    ),
    is_current = false
WHERE valid_to_utc = TIMESTAMP '9999-12-31 23:59:59'
  AND is_current = true
  AND EXISTS (
      SELECT 1 FROM {{ fact_table }} newer
      WHERE newer.event_bk = {{ fact_table }}.event_bk
        AND newer.valid_from_utc > {{ fact_table }}.valid_from_utc
  )

{% endif %}

{% endmacro %}
