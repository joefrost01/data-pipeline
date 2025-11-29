# Support Runbook

Operational procedures for the Markets Data Pipeline.

## Quick Reference

| Alert | First Action |
|-------|--------------|
| Source missing | Check upstream system status; contact source owner |
| Validation failed | Check `failed/` bucket for file and error log |
| dbt model failed | Check `control.dbt_runs` for error message |
| Regulatory late | Check `control.regulatory_reconciliation`; escalate to on-call |
| Streaming lag | Check Kafka consumer health; check GKE pod logs |
| Pipeline stale | Check GKE CronJob status; check pod logs |

---

## Alert: Source Missing

**Trigger:** Expected source file has not arrived by `expected_by` time.

**Steps:**

1. Check `control.source_completeness` for the source:
   ```sql
   SELECT * FROM control.source_completeness
   WHERE source_name = 'murex_trades'
     AND business_date = CURRENT_DATE()
   ```

2. Check if file arrived late (might already be processing):
   ```bash
   gsutil ls gs://markets-int-landing/murex/
   ```

3. Contact source owner (see `source_specs/{domain}/{source}.yaml` for owner email)

4. If known outage, acknowledge in ServiceNow — file will auto-process when it arrives

**Escalation:** If missing for 2+ consecutive days, escalate to P2.

---

## Alert: Validation Failed

**Trigger:** File failed schema or control file validation.

**Steps:**

1. Find the failed file:
   ```bash
   gsutil ls gs://markets-int-failed/
   ```

2. Check the error log:
   ```bash
   gsutil cat gs://markets-int-failed/{filename}.error.txt
   ```

3. Check validation details in BigQuery:
   ```sql
   SELECT * FROM control.validation_runs
   WHERE passed = FALSE
     AND DATE(run_timestamp) = CURRENT_DATE()
   ORDER BY run_timestamp DESC
   ```

4. Common failures:

   | Failure | Cause | Resolution |
   |---------|-------|------------|
   | Row count mismatch | Control file doesn't match data | Contact source owner |
   | Schema violation | Missing required field | Contact source owner |
   | Quarantined rows | Some rows failed validation | Check quarantine file, may be acceptable |

5. If source needs to resend, they drop corrected file in landing — it will process automatically

---

## Alert: dbt Model Failed

**Trigger:** A dbt model failed during the hourly run.

**Steps:**

1. Check which model failed:
   ```sql
   SELECT model_name, error_message, run_timestamp
   FROM control.dbt_runs
   WHERE status = 'error'
     AND DATE(run_timestamp) = CURRENT_DATE()
   ORDER BY run_timestamp DESC
   ```

2. Check GKE pod logs for full stack trace:
   ```bash
   kubectl logs -n surveillance -l app=surveillance-pipeline --since=2h | grep -A 20 "error"
   ```

3. Common failures:

   | Error | Cause | Resolution |
   |-------|-------|------------|
   | `Duplicate key` | Source sent duplicates | Check source data; may need manual dedup |
   | `NULL in non-nullable` | Bad source data | Check staging model filters |
   | `Table not found` | Missing dependency | Check if upstream model failed first |
   | `Quota exceeded` | BigQuery limit hit | Request quota increase |

4. To manually re-run dbt after fix:
   ```bash
   kubectl exec -it -n surveillance deploy/surveillance-pipeline -- dbt build --select model_name+
   ```

---

## Alert: Regulatory Late

**Trigger:** Regulatory submission exceeded 15-minute SLA.

**Priority:** P1 — Immediate escalation required.

**Steps:**

1. Check reconciliation status:
   ```sql
   SELECT * FROM control.regulatory_reconciliation
   WHERE status IN ('MISSED', 'LATE')
     AND DATE(event_timestamp) = CURRENT_DATE()
   ```

2. Check if regulatory reporter service is healthy:
   ```bash
   # Cloud Run
   gcloud run services describe markets-int-regulatory-reporter --region=europe-west2
   
   # Check logs
   gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=markets-int-regulatory-reporter" --limit=50
   ```

3. Check dead letter queue:
   ```sql
   SELECT * FROM control.regulatory_dead_letter
   WHERE resolved_at IS NULL
   ORDER BY failed_at DESC
   ```

4. If regulator API is down:
   - Events are being dead-lettered
   - They will need manual replay once API recovers
   - Escalate to regulatory reporting team

5. To manually replay dead-lettered events:
   ```bash
   # Contact engineering — requires manual script execution
   ```

---

## Alert: Streaming Lag

**Trigger:** Streaming pipeline lag exceeds 30 minutes.

**Steps:**

1. Check streaming health:
   ```sql
   SELECT * FROM control.streaming_health
   WHERE status != 'HEALTHY'
   ORDER BY hour DESC
   LIMIT 10
   ```

2. Check Kafka consumer lag (if accessible):
   ```bash
   kafka-consumer-groups --bootstrap-server $KAFKA_BROKERS --describe --group pubsub-bridge
   ```

3. Check GKE streaming bridge pods:
   ```bash
   kubectl get pods -n surveillance -l component=streaming-bridge
   kubectl logs -n surveillance -l component=streaming-bridge --tail=100
   ```

4. Common causes:

   | Cause | Resolution |
   |-------|------------|
   | Pod crashed | K8s will restart; check logs for root cause |
   | Kafka connection lost | Check network/firewall; restart pods |
   | High volume spike | Scale up bridge replicas |
   | Pub/Sub quota | Request quota increase |

5. To scale bridge:
   ```bash
   kubectl scale deployment streaming-bridge -n surveillance --replicas=5
   ```

---

## Alert: Pipeline Stale

**Trigger:** No pipeline run in 4+ hours.

**Steps:**

1. Check CronJob status:
   ```bash
   kubectl get cronjobs -n surveillance
   kubectl get jobs -n surveillance --sort-by=.metadata.creationTimestamp
   ```

2. Check if job is stuck:
   ```bash
   kubectl get pods -n surveillance -l app=surveillance-pipeline
   kubectl describe pod -n surveillance {pod-name}
   ```

3. Check recent job logs:
   ```bash
   kubectl logs -n surveillance job/{latest-job-name}
   ```

4. If stuck, delete the stuck job (CronJob will create new one next hour):
   ```bash
   kubectl delete job -n surveillance {stuck-job-name}
   ```

5. To trigger manual run:
   ```bash
   kubectl create job --from=cronjob/surveillance-pipeline manual-run-$(date +%s) -n surveillance
   ```

---

## Alert: Volume Anomaly

**Trigger:** Row count significantly higher or lower than normal (z-score > 3).

**Steps:**

1. Check the anomaly:
   ```sql
   SELECT * FROM control.row_count_anomalies
   WHERE DATE(business_date) = CURRENT_DATE()
     AND status IN ('CRITICALLY_LOW', 'CRITICALLY_HIGH')
   ```

2. Compare to recent history:
   ```sql
   SELECT source_name, business_date, daily_rows
   FROM control.source_completeness
   WHERE source_name = '{source}'
   ORDER BY business_date DESC
   LIMIT 14
   ```

3. Investigate:
   - **Critically low:** Partial file? Source outage? Holiday?
   - **Critically high:** Duplicate file? Backfill? Wrong file?

4. If legitimate (e.g., holiday), acknowledge and close

5. If suspicious, contact source owner before data reaches surveillance partner

---

## Useful Queries

**Today's pipeline status:**
```sql
SELECT
  'Validation' AS step,
  COUNT(*) AS total,
  COUNTIF(passed) AS passed,
  COUNTIF(NOT passed) AS failed
FROM control.validation_runs
WHERE DATE(run_timestamp) = CURRENT_DATE()

UNION ALL

SELECT
  'dbt' AS step,
  COUNT(*) AS total,
  COUNTIF(status = 'success') AS passed,
  COUNTIF(status = 'error') AS failed
FROM control.dbt_runs
WHERE DATE(run_timestamp) = CURRENT_DATE()
```

**Source arrival times today:**
```sql
SELECT
  source_name,
  MIN(run_timestamp) AS first_arrival,
  MAX(run_timestamp) AS last_arrival,
  COUNT(*) AS file_count,
  SUM(row_count) AS total_rows
FROM control.validation_runs
WHERE DATE(run_timestamp) = CURRENT_DATE()
  AND passed = TRUE
GROUP BY source_name
ORDER BY first_arrival
```

**Recent regulatory submissions:**
```sql
SELECT
  DATE(submitted_at) AS date,
  COUNT(*) AS submissions,
  AVG(submission_latency_seconds) AS avg_latency_seconds,
  MAX(submission_latency_seconds) AS max_latency_seconds,
  COUNTIF(submission_latency_seconds > 900) AS over_15min
FROM control.regulatory_submissions
WHERE submitted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY DATE(submitted_at)
ORDER BY date DESC
```

---

## Contacts

| Role | Contact |
|------|---------|
| Pipeline Engineering | data-engineering@company.com |
| Murex Support | trading-systems@company.com |
| Venue Integration | venue-integration@company.com |
| Reference Data | reference-data@company.com |
| Regulatory Reporting | regulatory-reporting@company.com |
| On-call Engineer | PagerDuty rotation |

---

## Reference

- [design.md](design.md) — Architecture and failure modes
- [GCP Console](https://console.cloud.google.com/home/dashboard?project=markets-int-12345)
- [Dynatrace Dashboard](https://your-tenant.live.dynatrace.com)
- [ServiceNow](https://company.service-now.com)