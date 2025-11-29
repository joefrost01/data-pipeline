# Streaming Architecture

How we handle high-volume, low-latency data from Kafka sources.

## Overview

Lane 2 (Streaming) handles sub-hour data that can't wait for the hourly batch cycle. The primary use case is events that feed the regulatory real-time alerting.

```
Kafka → Pub/Sub Bridge → Pub/Sub → BigQuery Storage Write API → dbt (dedupe) → Consumer
```

This is not a streaming analytics engine. We're not doing windowed aggregations or CEP (Complex Event Processing). We're doing **fast batch** — getting events into BigQuery within minutes so dbt can process them on the next hourly run, or so the regulatory reporter can act on them immediately.

---

## Why Not Direct Kafka → BigQuery?

BigQuery doesn't natively consume from Kafka. Options considered:

| Option | Pros | Cons |
|--------|------|------|
| Kafka Connect + BQ Sink | Mature, widely used | Another cluster to manage, Java ecosystem |
| Dataflow (Beam) | Managed, scalable | Complex for simple use case, expensive at low volume |
| Pub/Sub Bridge | Simple, GCP-native, cheap | Extra hop, slight latency |

We chose **Pub/Sub Bridge** because:

1. It's a single Python deployment we control
2. Pub/Sub → BigQuery is a managed subscription (zero code)
3. Total cost is ~£100/month at our volumes
4. We're already on GCP, so Pub/Sub is natural

If volumes grow 10x or latency requirements tighten, Dataflow becomes worth the complexity.

---

## Architecture

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│  Pub/Sub Bridge │────▶│   Pub/Sub   │────▶│  BigQuery   │
│  (source)   │     │   (GKE Pod)     │     │   Topic     │     │  raw.rfq_   │
└─────────────┘     └─────────────────┘     └─────────────┘     │  stream     │
                                                                └─────────────┘
                                                                       │
                                                                       ▼
                                                                ┌─────────────┐
                                                                │ dbt staging │
                                                                │ (dedupe)    │
                                                                └─────────────┘
```

### Pub/Sub Bridge

A lightweight Python service that:

1. Consumes from Kafka topics
2. Transforms messages to Pub/Sub format
3. Publishes to Pub/Sub topics
4. Tracks consumer offsets in Kafka

```python
# Simplified bridge logic
for message in kafka_consumer:
    pubsub_message = {
        "rfq_id": message.value["rfq_id"],
        "rfq_timestamp": message.value["timestamp"],
        "trader_id": message.value["trader"],
        # ... other fields
        "_kafka_partition": message.partition,
        "_kafka_offset": message.offset,
        "_kafka_timestamp": message.timestamp,
    }
    publisher.publish(topic, json.dumps(pubsub_message).encode())
```

We preserve Kafka metadata (`_kafka_partition`, `_kafka_offset`) for:
- Debugging delivery issues
- Detecting sequence gaps
- Deduplication

### Pub/Sub → BigQuery Subscription

Pub/Sub has a native BigQuery subscription type. No code required:

```hcl
# terraform/int/pubsub.tf
resource "google_pubsub_subscription" "rfq_stream_bq" {
  name  = "markets-int-rfq-stream-bq"
  topic = google_pubsub_topic.rfq_stream.id

  bigquery_config {
    table            = "${var.project_id}.raw.rfq_stream"
    use_table_schema = true
    write_metadata   = true
  }
}
```

Messages land directly in `raw.rfq_stream` as rows.

---

## Delivery Semantics

### At-Least-Once Delivery

Both Kafka and Pub/Sub provide **at-least-once** delivery. This means:

- Every message is delivered
- Some messages may be delivered more than once

Duplicates occur when:
- Bridge crashes after publishing but before committing offset
- Pub/Sub retries on transient BigQuery errors
- Network partitions cause redelivery

### Deduplication Strategy

We handle duplicates in the dbt staging model:

```sql
-- models/staging/stg_rfqs.sql

with deduplicated as (
    select
        *,
        row_number() over (
            partition by rfq_id
            order by _ingestion_time asc
        ) as _row_num
    from {{ source('raw', 'rfq_stream') }}
)

select * from deduplicated
where _row_num = 1
```

This keeps the **first arrival** of each `rfq_id`. We use first (not last) because:
- The first arrival has the lowest latency
- Later arrivals are duplicates, not updates
- For RFQs, there are no amendments — an RFQ is immutable

### Why Not Exactly-Once?

Exactly-once requires:
- Transactional writes across Kafka and Pub/Sub (not supported)
- Or: idempotent writes with deduplication at the sink

We chose sink-side deduplication because:
- It's simpler to implement
- It's already needed for batch sources (reprocessing)
- BigQuery handles it efficiently with `row_number()`

---

## Throughput and Latency

### Current Capacity

| Metric | Target | Actual |
|--------|--------|--------|
| Messages/day | 300M | ~150M (headroom) |
| Peak messages/sec | 5,000 | ~3,000 |
| Ingestion latency (Kafka → BQ) | <30 sec | p50: 5s, p99: 15s |
| End-to-end (Kafka → staging) | <5 min | ~3 min (next dbt run) |

### Scaling the Bridge

The bridge runs as a Kubernetes Deployment:

```yaml
# k8s/streaming-bridge.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-bridge
spec:
  replicas: 3  # Scale horizontally
  template:
    spec:
      containers:
      - name: bridge
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
```

Each replica consumes from a subset of Kafka partitions. To scale:

```bash
kubectl scale deployment streaming-bridge -n markets --replicas=5
```

### Kafka Partition Alignment

For optimal throughput, match bridge replicas to Kafka partitions:

| Kafka Partitions | Bridge Replicas | Notes |
|------------------|-----------------|-------|
| 6 | 3 | 2 partitions per replica |
| 12 | 6 | 2 partitions per replica |
| 12 | 12 | 1 partition per replica (max parallelism) |

More replicas than partitions wastes resources (idle consumers).

---

## Gap Detection

We monitor for missing messages using Kafka sequence IDs.

### Source Requirements

Sources must provide a monotonically increasing sequence ID:

```json
{
  "rfq_id": "RFQ-2025-001234",
  "sequence_id": 98765,
  ...
}
```

### Detection Logic

The `streaming_sequence_gaps` model detects non-consecutive sequences:

```sql
-- models/control/streaming_sequence_gaps.sql

with sequences as (
    select
        source_sequence_id,
        lag(source_sequence_id) over (
            partition by _kafka_partition
            order by source_sequence_id
        ) as prev_sequence_id
    from {{ ref('stg_rfqs') }}
    where source_sequence_id is not null
),

gaps as (
    select
        prev_sequence_id + 1 as gap_start,
        source_sequence_id - 1 as gap_end,
        source_sequence_id - prev_sequence_id - 1 as gap_size
    from sequences
    where source_sequence_id - prev_sequence_id > 1
)

select * from gaps
```

### Alerting

Gaps are classified by severity:

| Gap Size | Severity | Action |
|----------|----------|--------|
| 1-10 | LOW | Log only |
| 11-100 | MEDIUM | Alert, investigate within 24h |
| 101-1000 | HIGH | Alert, investigate within 4h |
| 1000+ | CRITICAL | Page on-call, immediate investigation |

Large gaps typically indicate:
- Kafka consumer fell behind and was reset
- Network partition between Kafka and bridge
- Upstream source had an outage

---

## Schema Enforcement

### At Ingestion

The Pub/Sub → BigQuery subscription uses `use_table_schema = true`. Messages with unknown fields or wrong types are rejected.

We do **not** use `IGNORE_UNKNOWN_VALUES` because:
- Schema changes should be explicit
- Unknown fields might indicate upstream problems
- We want to fail fast, not silently drop data

### Schema Evolution

To add a field:

1. Add column to BigQuery table (Terraform)
2. Deploy bridge with new field mapping
3. Add field to staging model

To remove a field:

1. Remove from bridge mapping
2. Remove from staging model
3. Optionally drop column from BQ (or leave as NULL)

Backwards-incompatible changes (type changes, removing required fields) require coordination with the source team.

---

## Failure Handling

### Bridge Crashes

Kubernetes restarts the pod. On restart:
- Consumer rejoins group
- Rebalances partitions
- Resumes from last committed offset

Some messages may be redelivered (at-least-once). Deduplication handles this.

### Pub/Sub Backlog

If BigQuery is slow or unavailable, messages queue in Pub/Sub:

- Retention: 7 days
- Alerting: Backlog age > 30 minutes

To drain a backlog:
```bash
# Check backlog
gcloud pubsub subscriptions describe markets-int-rfq-stream-bq \
  --format="value(numUndeliveredMessages)"

# BigQuery will auto-drain when healthy
# No manual intervention needed
```

### BigQuery Quota

Streaming inserts have quotas:
- 1TB/day per table (default)
- 100,000 rows/sec per table

We've requested quota increases for production. Monitor via:
```sql
SELECT * FROM `region-europe-west2`.INFORMATION_SCHEMA.STREAMING_TIMELINE
WHERE table_name = 'rfq_stream'
ORDER BY start_time DESC
LIMIT 100
```

### Dead Letter Queue

Messages that fail repeatedly (5 attempts) go to dead letter:

```hcl
dead_letter_policy {
  dead_letter_topic     = google_pubsub_topic.dead_letter.id
  max_delivery_attempts = 5
}
```

Dead-lettered messages need manual investigation:
```bash
# Pull dead letter messages
gcloud pubsub subscriptions pull markets-int-dead-letter-pull --limit=10 --auto-ack=false
```

---

## Monitoring

### Key Metrics

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| Consumer lag | Kafka | > 10,000 messages |
| Pub/Sub backlog age | Pub/Sub | > 30 minutes |
| Ingestion latency | BigQuery | p99 > 60 seconds |
| Gap count (daily) | control.streaming_sequence_gaps | > 10 |
| Dead letter count | Pub/Sub | > 0 |

### Dashboards

Dynatrace dashboard includes:
- Messages/sec through bridge
- Kafka consumer lag by partition
- Pub/Sub backlog depth
- BigQuery streaming insert latency
- Gap count over time

### Health Check

The bridge exposes `/health`:

```python
@app.get("/health")
def health():
    return {
        "status": "healthy",
        "kafka_connected": consumer.is_connected(),
        "pubsub_connected": publisher.is_connected(),
        "last_message_at": last_message_time.isoformat(),
        "messages_processed": message_count,
    }
```

Kubernetes uses this for liveness/readiness probes.

---

## Comparison with Batch

| Aspect | Batch (Lane 1) | Streaming (Lane 2) |
|--------|---------------|-------------------|
| Latency | 1-2 hours | 1-5 minutes |
| Delivery | File-based, exactly-once | Message-based, at-least-once |
| Deduplication | Deterministic ID + merge | Deterministic ID + row_number |
| Schema | Parquet (strongly typed) | JSON → BigQuery (schema enforced) |
| Backfill | Re-drop file | Replay from Kafka (if retained) |
| Cost | £0.01/GB (GCS) | £0.10/GB (Pub/Sub + streaming inserts) |

Use streaming when:
- Data needed within minutes
- Source is event-driven (Kafka, webhooks)
- Regulatory latency SLAs apply

Use batch when:
- Hourly latency is acceptable
- Source is file-based
- Cost sensitivity is high

---

## Adding a Streaming Source

1. **Kafka topic access** — Coordinate with source team for topic name, credentials, schema

2. **Bridge configuration** — Add topic to bridge config:
   ```yaml
   # config/streaming_sources.yaml
   - name: new_events
     kafka_topic: upstream.new.events
     pubsub_topic: markets-int-new-events
     key_field: event_id
     schema:
       - name: event_id
         type: STRING
       - name: event_time
         type: TIMESTAMP
       # ...
   ```

3. **Pub/Sub resources** — Add to Terraform:
   ```hcl
   resource "google_pubsub_topic" "new_events" { ... }
   resource "google_pubsub_subscription" "new_events_bq" { ... }
   ```

4. **BigQuery table** — Add schema to `terraform/int/schemas/`

5. **dbt staging model** — Create `stg_new_events.sql` with deduplication

6. **Test** — Publish test messages, verify they land in staging

---

## Future Considerations

### If Latency Requirements Tighten (<1 min)

- Move deduplication to Dataflow (streaming SQL)
- Use BigQuery Storage Write API directly from bridge
- Consider Bigtable for sub-second lookups

### If Volume Grows 10x

- Increase Kafka partitions and bridge replicas
- Request higher BigQuery streaming quotas
- Consider Dataflow for managed scaling

### If We Need Streaming Joins

- Dataflow with windowed joins
- Or: Materialize/ksqlDB at the Kafka layer
- Current architecture doesn't support this

---

## Summary

Our streaming architecture is deliberately simple:

1. **Bridge** — Kafka to Pub/Sub, preserving metadata
2. **Pub/Sub** — Managed delivery to BigQuery
3. **BigQuery** — Storage with schema enforcement
4. **dbt** — Deduplication and transformation

It handles 300M messages/day with sub-5-minute latency, costs ~£100/month, and requires no complex streaming infrastructure.

The key insight: we don't need a streaming *analytics* engine. We need fast delivery to a batch system. Pub/Sub + BigQuery achieves that with minimal operational burden.