# Markets Data Products Pipeline

A modern, lightweight data platform for Markets using BigQuery, dbt, and minimal orchestration. Designed for simplicity, auditability, and operational excellence.

**Version:** 0.2  
**Status:** Draft for review

---

## Who This Document Is For

| Audience | Relevant Sections |
|----------|-------------------|
| Data Engineering | Full document |
| Trade Surveillance | Overview, Data Flow, Extract Generation |
| Ops Support | Support Runbook, Observability, Failure Modes |
| Security | IAM, Audit & Compliance, Identity Management |
| Architecture Review Boards | Core Principles, Technology Stack, Architecture |
| Risk & Compliance | Audit & Compliance, Regulatory Reporting, Control Tables |

---

## Table of Contents

- [Overview](#overview)
- [Why BigQuery + dbt + Minimal Orchestration](#why-bigquery--dbt--minimal-orchestration)
- [Architecture](#architecture)
- [Core Principles](#core-principles)
- [Non-Goals](#non-goals)
- [Technology Stack](#technology-stack)
- [Data Flow](#data-flow)
- [Day in the Life of Data](#day-in-the-life-of-data)
- [Identity Management](#identity-management)
- [Ingestion Patterns](#ingestion-patterns)
- [How to Onboard a New Source](#how-to-onboard-a-new-source)
- [Project Structure](#project-structure)
- [Error Handling Philosophy](#error-handling-philosophy)
- [Failure Modes & Recovery](#failure-modes--recovery)
- [Dimension Strategy](#dimension-strategy)
- [Streaming Reconciliation](#streaming-reconciliation)
- [Extract Generation](#extract-generation)
- [Regulatory Reporter](#regulatory-reporter)
- [Audit & Compliance](#audit--compliance)
- [Observability & Alerting](#observability--alerting)
- [Support Runbook](#support-runbook)
- [Cost Model](#cost-model)
- [FAQ](#faq)
- [Glossary](#glossary)

---

## Overview

This platform ingests trade data from multiple upstream sources (venues, OMS, Bloomberg, Kafka streams, etc.), validates and transforms it through a series of layers, and produces daily extracts for our third-party Trade Surveillance partner.

The design philosophy is **simplicity over complexity**. We use BigQuery as both storage and compute engine, dbt for all transformations, and a minimal Python orchestrator. The architecture supports both batch and streaming ingestion patterns, choosing the simplest approach that meets latency requirements.

### Key Outcomes

- **Single repository** containing all pipeline code, tests, documentation, and configuration
- **Hourly batch processing** with incremental models — always up to date
- **Streaming capability** for high-volume or low-latency sources
- **Full audit trail** of every file and message received, validated, and processed
- **Self-healing pipeline** — drop a file in landing, it gets processed
- **Operationally simple** — support team works entirely in Dynatrace/ServiceNow
- **Deterministic identity** — globally unique, reproducible trade IDs

---

## Why BigQuery + dbt + Minimal Orchestration

> **Design Decision**
>
> We chose BigQuery as our warehouse, dbt as our transformation engine, and a simple Python orchestrator over more complex alternatives. Here's why:
>
> - **Operational simplicity** — No DAG server to maintain, no Spark clusters to tune
> - **Onboarding simplicity** — Any engineer who knows SQL can read and modify the pipeline
> - **Cost stability** — Pay-per-query with predictable incremental processing costs
> - **Ecosystem fit** — Native GCP integration with existing LBG cloud infrastructure
> - **Maintainability** — Small team can own and operate without specialist skills
> - **Auditability** — Everything in SQL is inspectable, testable, and version-controlled
> - **Single language for logic** — All business rules live in SQL/dbt models rather than being split across Python, Java, or bespoke frameworks, which reduces cognitive load and simplifies change control.

---

## Architecture

The platform comprises three loosely coupled subsystems with different operational characteristics.
A core design principle is that these three lanes are operationally independent: failure, backlog, or degradation in one lane must not block or materially impact the others:
```
═══════════════════════════════════════════════════════════════════════════════════
                              THREE PARALLEL LANES
═══════════════════════════════════════════════════════════════════════════════════

LANE 1: BATCH TRADE PROCESSING (Core)
──────────────────────────────────────
SLA: Hourly processing, 7-day rolling window for surveillance
Complexity: Low — file-based, linear, predictable
────────────────────────────────────────────────────────────────────────────────────
  Source Files → GCS Landing → Validator → GCS Staging → BigQuery (dbt) → Extract
────────────────────────────────────────────────────────────────────────────────────


LANE 2: HIGH-VOLUME STREAMING INGESTION
───────────────────────────────────────
SLA: Sub-hour latency, eventually consistent with batch
Complexity: Medium — requires monitoring, sequence gap detection
────────────────────────────────────────────────────────────────────────────────────
  Kafka → Pub/Sub Bridge → BigQuery Streaming Insert → dbt (hourly merge)
────────────────────────────────────────────────────────────────────────────────────


LANE 3: LOW-LATENCY REGULATORY REPORTING
────────────────────────────────────────
SLA: <15 minutes from event to submission (regulatory deadline)
Complexity: Higher — real-time, requires cache, separate failure handling
────────────────────────────────────────────────────────────────────────────────────
  Kafka/Pub/Sub → Regulatory Reporter (Cloud Run/GKE) → Regulator API
                                                      → Audit Table (reconciliation)
────────────────────────────────────────────────────────────────────────────────────

═══════════════════════════════════════════════════════════════════════════════════
```

### Detailed Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                   Source Systems                                    │
│    Venue A │ Venue B │ Bloomberg │ Internal OMS │ Reference Data │ Kafka Streams    │
└──────┬─────┴────┬────┴─────┬─────┴──────┬───────┴───────┬────────┴────────┬─────────┘
       │          │          │            │               │                 │
       │          │          │            │               │                 │
       ▼          ▼          ▼            ▼               ▼                 │
┌─────────────────────────────────────────────────────────────────┐         │
│                         GCS: landing/                           │         │
│                   (files arrive here untouched)                 │         │
└───────────────────────────────┬─────────────────────────────────┘         │
                                │                                           │
       ┌────────────────────────┤                                           │
       │                        │                                           │
       ▼                        ▼                                           ▼
┌──────────────────┐  ┌─────────────────────┐              ┌────────────────────────────┐
│    Validator     │  │  Kafka → GCS Sink   │              │    Kafka → Pub/Sub         │
│                  │  │  (low-volume batch) │              │    (high-volume/low-lat)   │
│  • Source specs  │  │                     │              │                            │
│  • Schema check  │  │  • Batches messages │              │  • Kafka Connect or        │
│  • Control files │  │  • Writes to GCS    │              │    consumer group          │
│  • Moves files   │  │  • Same validation  │              │  • Real-time bridge        │
└────────┬─────────┘  └──────────┬──────────┘              └─────────────┬──────────────┘
         │                       │                                       │
         ▼                       ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                    BigQuery                                         │
│                                                                                     │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────┐          │
│  │    raw      │    │   staging   │    │  curation   │    │   consumer   │          │
│  │  (external  │───▶│   (clean,   │───▶│ (enriched,  │───▶│  (mart tables│          │
│  │   tables +  │    │   typed)    │    │  joined)    │    │   & views)   │          │
│  │  streaming) │    │             │    │             │    │              │          │
│  └─────────────┘    └─────────────┘    └─────────────┘    └──────┬───────┘          │
│         ▲                                                        │                  │
│         │                                                        │                  │
│         │           ┌───────────────────────────────────────────────────────────┐   │
│  Pub/Sub → BQ       │                    control schema                         │   │
│  Streaming Insert   │  • validation_runs    • dbt_runs  • source_completeness   │   │
│                     │  • row_count_anomalies            • ingested_files        │   │
│                     │  • streaming_sequence_gaps        • regulatory_submissions│   │
│                     └───────────────────────────────────────────────────────────┘   │
│                                                                                     │
│  ┌───────────────────────────────────────────────────────────────────────────────┐  │
│  │                           snapshots schema                                    │  │
│  │  • traders_snapshot (SCD-2)    • counterparties_snapshot (SCD-2)              │  │
│  │  • books_snapshot (SCD-2)      • instruments_snapshot (SCD-1)                 │  │
│  └───────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         │  dbt build (hourly)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              Extract Generation                                     │
│                                                                                     │
│  • 7-day rolling window                                                             │
│  • JSON/JSONL format (configurable) for surveillance partner                        │
│  • bq extract → GCS: extracts/                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           Surveillance Partner                                      │
│                        (MAR detection on 7-day window)                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Principles

1. **Simplicity is the #1 Feature** — Every component should be understandable by a new team member within a day.

2. **SQL is the Universal Language** — All transformations and business rules are implemented in SQL via dbt models. No Spark, no custom frameworks, no split logic between Python and SQL.

3. **Configuration over Code** — Adding a new source requires only a YAML file, not code changes.

4. **Everything is Auditable** — Every file, validation, and dbt run is logged to BigQuery.

5. **Self-Healing by Default** — Late files are picked up on the next run. The 7-day window provides resilience.

6. **Operability First** — Support teams work in Dynatrace and ServiceNow, not GCP.

7. **Deterministic Identity** — Trade IDs are generated reproducibly from source identifiers.

8. **Right Tool for the Latency** — Batch for hourly SLAs, streaming for sub-hour, real-time for regulatory deadlines.

---

## Non-Goals

This platform is explicitly **not**:

| Non-Goal | Rationale                                                             |
|----------|-----------------------------------------------------------------------|
| A general-purpose ETL tool | Purpose-built for Markets; other use cases need separate pipelines    |
| A lineage/catalogue tool | Use Dataplex or similar for enterprise lineage                        |
| A BI modelling layer | Surveillance extracts only; BI teams build their own marts            |
| A streaming analytics engine | Batch reconciliation is the source of truth; streaming is for latency |
| A recreation of golden sources | We ingest from golden sources; we don't replace them                  |
| A long-term operational datastore | 7-day rolling window; historical archives are separate                |
| A replacement for Murex BO | Murex remains the system of record for trade lifecycle                |

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Storage | GCS | File landing, staging, archive, failed |
| Warehouse | BigQuery | All data storage and compute |
| Transforms | dbt-core + dbt-bigquery | SQL transformations, tests, docs |
| Orchestration | Python + GKE CronJob | Simple scheduling and coordination |
| Streaming Bridge | Pub/Sub | Kafka-to-BigQuery streaming |
| Real-time Services | Cloud Run / GKE | Low-latency regulatory reporting |
| Secrets | GCP Secret Manager / KMS | API tokens, credentials |
| IAM | GCP IAM | Service account permissions |
| Monitoring | Dynatrace | Metrics, logs, alerting |
| Ticketing | ServiceNow | Incident management |

### What We Don't Use (and Why)

| Technology | Why Not |
|------------|---------|
| Composer/Airflow | Overkill for linear pipelines; operational overhead |
| Dataflow/Spark | BigQuery handles our scale; unnecessary complexity |
| Data Fusion | GUI-based ETL; not version controllable |
| Dataplex | Not GA; dbt provides equivalent DQ and lineage |
| Informatica | Legacy; not cloud-native; expensive |

---

## Data Flow

### Hourly Batch Pipeline

The orchestrator (`orchestrator/main.py`) runs hourly via GKE CronJob and executes:

```
Every hour (CronJob, concurrencyPolicy: Forbid):
│
├─1─▶ validator.py runs
│     • Scans landing/ for new files
│     • Validates against source specs (schema, control files)
│     • Converts XML → Parquet where applicable
│     • Moves valid → staging/, invalid → failed/
│     • Writes to control.validation_runs
│     • Pushes metrics to Dynatrace
│
├─2─▶ dbt build runs
│     • External tables read from staging/
│     • Streaming tables read from raw.*_stream
│     • Incremental models process new rows only
│     • Tests run on all models
│     • Results logged to control.dbt_runs
│
├─3─▶ archive_processed_files.py runs
│     • Moves staging/ → archive/{date}/{time}/
│     • Files arriving during run remain in landing/ for next cycle
│
├─4─▶ extract_generation.py runs (06:00 UTC only)
│     • Extracts 7-day rolling window to GCS
│     • Format: newline-delimited JSON (configurable via EXTRACT_FORMAT env var)
│
└─5─▶ Metrics pushed to Dynatrace
```

### Streaming Pipeline (High-Volume Sources)

All large BigQuery tables in this design are partitioned by business date (e.g. trade or event timestamp) and clustered by high-cardinality keys such as `trader_id`, `instrument`, or `event_id`. This ensures predictable query performance, efficient pruning, and stable cost even as data volumes grow.

```
Continuous:
│
├─1─▶ Kafka consumer reads messages
│     • From high-volume topics (e.g., RFQ stream)
│     • Republishes to Pub/Sub (consumer group: pubsub-bridge)
│     • Multiple pods for throughput (partition-based)
│
├─2─▶ Pub/Sub → BigQuery streaming insert
│     • Uses BigQuery Storage Write API
│     • Schema validation: IGNORE_UNKNOWN_VALUES=false
│     • Lands in raw.*_stream tables
│     • At-least-once delivery (dedupe in staging)
│
├─3─▶ dbt picks up in next hourly run
│     • Staging models dedupe by event_id + _ingestion_time
│     • Incremental merge into curation layer
│
└─4─▶ Sequence gap detection (hourly)
      • Identifies missing sequence IDs
      • Alerts if gap_size > 100
```

**Streaming Specifics:**

| Parameter | Value |
|-----------|-------|
| Expected throughput | ~300M messages/day (RFQ stream) |
| Delivery semantics | At-least-once (dedupe in dbt) |
| Duplicate handling | `QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingestion_time) = 1` |
| Ingestion latency | Typically <5 seconds from Kafka to BigQuery |
| BigQuery streaming quota | 1TB/day per table (well within limits) |

BigQuery streaming quotas have been reviewed against expected RFQ and event volumes. Any increase required for production will be requested via the standard GCP quota process before go-live.

---

## Day in the Life of Data

A narrative flow of how a trade moves through the system:

1. **10:07** — Murex drops `trades_20251128_1007.xml` into `gs://markets-data/landing/murex/`

2. **11:00** — Hourly CronJob fires. Validator finds the file, parses XML, checks row count against sidecar control file, converts to Parquet, moves to `staging/murex/trades_20251128_1007.parquet`

3. **11:01** — dbt build starts. External table `raw.murex_trades` reads the new Parquet file. Staging model `stg_murex_trades` applies type casting and basic filtering

4. **11:02** — Curation model `trades_enriched` joins to dimension snapshots, generates deterministic `trade_id` using MD5 hash

5. **11:03** — Consumer model `surveillance_extract` unions all trade sources, applies final business rules

6. **11:04** — dbt tests run. Row counts logged to `control.dbt_runs`. Pipeline completes

7. **11:05** — Archive script moves processed files to `archive/2025-11-28/1100/`

8. **06:00 (next day)** — Extract generation creates `surveillance_extract_20251129.jsonl.gz`, transfers to partner

---

## Identity Management

> For detailed implementation, see [`docs/identity.md`](docs/identity.md)

### Trade Identity Contract

- `trade_id` is a **deterministic 32-character hex string** generated at the curation layer
- Generated per **logical trade**, persisted forever
- **Not regenerated** on amendments — same source trade = same trade_id
- Source identity retained as `source_trade_id` and `source_system`

### Deterministic ID Generation

We use MD5 hashing (not UUID5) for simplicity and BigQuery compatibility:

```
trade_id = MD5("{namespace}:{domain}:{source_system}:{source_trade_id}")
```

This produces a 32-character hex string that is:
- **Deterministic** — same inputs always produce same output
- **Collision-resistant** — different inputs produce different outputs
- **Efficient** — native BigQuery function, no UDF required

**Note on UUID5:** UUID5 uses SHA-1 with specific byte formatting. If you need to generate IDs
in Python that match the dbt macro, use `hashlib.md5()`, not `uuid.uuid5()`. See 
[`docs/identity.md`](docs/identity.md) for Python examples.

**CI Validation:** The namespace is linted in CI. A test verifies `generate_trade_id("markets", "MUREX", "TRD-12345")` always returns the same value.

---

## Ingestion Patterns

### Decision Framework

```
                         ┌─────────────────────┐
                         │    New Source       │
                         └──────────┬──────────┘
                                    │
                         ┌──────────▼──────────┐
                         │  Latency < 1 hour?  │
                         └──────────┬──────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                   YES                              NO
                    │                               │
         ┌──────────▼──────────┐         ┌─────────▼─────────┐
         │ Latency < 15 min?   │         │   Batch Pattern   │
         └──────────┬──────────┘         │   (file → GCS)    │
                    │                    └───────────────────┘
        ┌───────────┴───────────┐
        │                       │
       YES                      NO
        │                       │
┌───────▼───────┐    ┌──────────▼──────────┐
│ Real-time     │    │ Streaming Pattern   │
│ Service       │    │ (Pub/Sub → BQ)      │
│ (Cloud Run)   │    │                     │
└───────────────┘    └─────────────────────┘
```

### Batch Ingestion

For sources delivering files (CSV, JSON, XML, Parquet) with hourly SLAs.

**Source Specification Example:** See [`source_specs/trading/murex_trades.yaml`](source_specs/trading/murex_trades.yaml)

**XML Processing Note:** The validator converts XML to Parquet during validation. For files >500MB, consider requesting Parquet directly from source or using chunked processing. See [`docs/xml_processing.md`](docs/xml_processing.md) for implementation details.

### Streaming Ingestion

For high-volume sources (millions of messages/day) or sources natively publishing to Kafka.

**Kafka Bridge Scaling:** The bridge runs as a Kubernetes Deployment with multiple replicas. Each replica joins the same consumer group and is assigned partitions automatically. For >500M messages/day, ensure sufficient replicas and consider Kafka Connect (GCP-managed) for operational simplicity.

See [`docs/streaming_sources.md`](docs/streaming_sources.md) for full implementation.

---

## How to Onboard a New Source

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ONBOARDING A NEW SOURCE                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │  1. Create source spec YAML    │
                    │     source_specs/{domain}/     │
                    │     {source_name}.yaml         │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │  2. Create dbt staging model   │
                    │     models/staging/            │
                    │     stg_{source_name}.sql      │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │  3. Add to sources.yml         │
                    │     Define source + columns    │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │  4. Run smoke test             │
                    │     make test-source           │
                    │     SOURCE=source_name         │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │  5. (If streaming) Configure   │
                    │     Kafka bridge or GCS sink   │
                    └────────────────────────────────┘
```

**Smoke Test:** `make test-source SOURCE=new_venue` drops a sample file in a dev bucket and asserts it appears in `consumer.surveillance_extract` after one pipeline run.

See [`docs/adding_new_source.md`](docs/adding_new_source.md) for detailed walkthrough.

---

## Project Structure

```
markets-pipeline/
│
├── README.md                     # This file
├── pyproject.toml                # Python dependencies
│
├── source_specs/                 # Source validation specifications
│   ├── trading/
│   ├── market_data/
│   ├── streaming/
│   └── reference/
│
├── validator/                    # File validation
├── streaming/                    # Streaming components
├── regulatory_reporter/          # Low-latency reporting
├── orchestrator/                 # Pipeline orchestration
│
├── dbt_project/                  # dbt project
│   ├── models/
│   │   ├── staging/              # Clean, typed source data
│   │   ├── curation/             # Enriched, joined data (trade_id created here)
│   │   ├── consumer/             # Mart tables for consumption
│   │   ├── dimensions/           # Reference dimensions
│   │   └── control/              # Pipeline control models
│   ├── snapshots/                # SCD Type 2
│   ├── seeds/                    # Static reference data
│   ├── macros/                   # Reusable SQL (including generate_trade_id)
│   └── tests/                    # Custom tests
│
├── terraform/                    # Infrastructure as code (source of truth)
│
└── docs/                         # Additional documentation
    ├── support_runbook.md
    ├── adding_new_source.md
    ├── streaming_sources.md
    ├── identity.md
    ├── xml_processing.md
    └── architecture_decisions.md
```

All business transformation logic and business rules are implemented in dbt models under version control. Pipeline Python code is responsible for orchestration, validation, and movement of data only, not for implementing business rules.

---

## Error Handling Philosophy

### Pipeline Behaviour on Failure

| Failure Type | Behaviour | Rationale |
|--------------|-----------|-----------|
| **Single source file fails validation** | Continue processing other files | One bad file shouldn't block the entire pipeline |
| **dbt model fails** | Abort downstream of failed model; continue independent branches | Maintains data consistency while maximising throughput |
| **Source is late** | Process available sources; alert after expected_by time | Self-healing — file processed on next run |
| **Malformed rows in file** | Quarantine row-by-row to `failed/` with reason; process valid rows | Maximise data availability while preserving evidence |
| **Streaming lag exceeds threshold** | Alert; continue processing | Batch reconciliation catches up |
| **Regulatory reporter fails** | Retry with exponential backoff; dead-letter after 3 attempts | Regulatory submissions are critical |

### Validation Behaviour

The validator operates on a **fail-fast per file, continue per run** basis:

- If a file has structural issues (wrong format, missing control file), the entire file fails
- If individual rows fail validation, valid rows proceed and invalid rows are quarantined
- Quarantined rows include: file path, row number, raw content, failure reason, timestamp

---

## Failure Modes & Recovery

### Availability, RTO and RPO

- **Batch and streaming lanes**
  - Target availability: 99.5%
  - Recovery Time Objective (RTO): < 1 hour for restoring normal pipeline operation after an incident
  - Recovery Point Objective (RPO): < 1 hour for batch data; < 15 minutes for streaming data (replayed from Kafka or source files where applicable)

- **Regulatory reporting lane**
  - Target availability: 99.9%
  - RTO: < 30 minutes for restoring submission capability
  - RPO: < 15 minutes, limited by upstream event availability and regulator SLAs


| Failure Mode | Likelihood | Impact | Detection | Recovery |
|--------------|------------|--------|-----------|----------|
| **Late files** | Common | Low | `source_completeness` table; Dynatrace alert | Auto-heals on next run within 7-day window |
| **Corrupt files** | Rare | Medium | Validator fails; file moved to `failed/` | Contact source owner; resubmit corrected file |
| **Duplicate files** | Rare | Low | Deterministic IDs prevent duplicate trades | No action needed — idempotent |
| **Streaming lag** | Occasional | Medium | `streaming_health` model; lag > 30min alert | Check Kafka consumer health; scale replicas |
| **Regulatory reporter outage** | Rare | High | `regulatory_reconciliation` shows MISSED | See [Regulatory Reporter Failure Handling](#regulatory-reporter-failure-handling) |
| **BigQuery quota exceeded** | Very Rare | High | GCP quota alerts | Request quota increase; reduce query concurrency |
| **GKE pod crash** | Occasional | Low | K8s restarts pod; Dynatrace alerts | Auto-recovers; check logs if repeated |
| **Missing reference data** | Rare | Medium | dbt test failures (referential integrity) | Investigate upstream; manual seed if urgent |
| **Kafka → Pub/Sub bridge bottleneck** | Rare (>500M/day) | High | Consumer lag metrics | Scale bridge pods; consider Kafka Connect managed |
| **Deterministic UUID collision** | Extremely Rare | Catastrophic | CI tests namespace stability | Namespace UUID is immutable; CI enforces |

### What Happens When Things Break

**Scenario: Murex file arrives 3 hours late**
1. 06:00 expected_by passes → Dynatrace alert "Source missing: murex_trades"
2. Support acknowledges; contacts Murex team
3. 09:30 file arrives in landing/
4. 10:00 hourly run picks it up automatically
5. Trades appear in surveillance_extract; 7-day window means no data loss

**Scenario: Validator pod OOMs on large XML file**
1. Pod crashes; K8s restarts it
2. File remains in landing/ (not yet moved)
3. Next run reattempts validation
4. If repeated: Dynatrace alert escalates; engineer investigates
5. Fix: Increase pod memory or split file at source

### Disaster Recovery

BigQuery, GCS, Pub/Sub, and Cloud Run are all multi-zone managed services; GKE clusters are deployed across multiple zones within a single region. Cross-region DR is not required for this data pipeline: in the event of a regional outage, RTO and RPO are determined by the bank’s broader GCP regional failover strategy rather than this solution specifically.

---

## Dimension Strategy

Dimensions use appropriate SCD strategies based on business requirements.

### SCD Type 2 (Full History)

Used for dimensions where historical accuracy is required:

| Dimension | Rationale                                                                  |
|-----------|----------------------------------------------------------------------------|
| `dim_counterparty` | Legal entity changes affect regulatory reporting                           |
| `dim_book` | Book hierarchy changes affect P&L and risk                                 |
| `dim_trader` | Trader desk moves require historical accuracy for things like surveillance |

### SCD Type 1 (Current State Only)

| Dimension | Rationale |
|-----------|-----------|
| `dim_instrument` | Millions of instruments; SCD-2 would multiply storage significantly |

**Mitigation for `dim_instrument` historical needs:**
- Trade snapshots capture instrument attributes at trade time
- Separate `instrument_history` table for instrument-specific analysis

---

## Streaming Reconciliation

### Sequence Gap Detection

When sources provide sequential IDs, we detect missing messages via hourly reconciliation.

Gaps are logged to `control.streaming_sequence_gaps` with severity (LOW/MEDIUM/HIGH/CRITICAL based on gap_size).

**Note:** Gaps are uncommon with properly configured Kafka consumers. When they occur, causes include: consumer restarts with incorrect offset management, network partitions, or upstream system issues.

See [`docs/streaming_sources.md`](docs/streaming_sources.md) for implementation details.

---

## Extract Generation

Daily at 06:00 UTC, the pipeline extracts a 7-day rolling window:

- **Format:** Newline-delimited JSON (`.jsonl.gz`) by default
- **Configurable:** Set `EXTRACT_FORMAT` env var to `avro` or `jsonl` as needed
- **Destination:** `gs://markets-data/extracts/{date}/surveillance_extract_{date}.jsonl.gz`
- **Transfer:** Automated transfer to surveillance partner via SFTP

---

## Regulatory Reporter

A dedicated streaming service for sub-15-minute regulatory submission SLAs.

The regulatory reporting lane is deliberately isolated from the batch and streaming ingestion lanes: failures or backlogs in regulatory submission processing must not block Kafka ingestion, Pub/Sub delivery, or the core batch pipeline.

### SLAs and Behaviour

| Metric | Target |
|--------|--------|
| End-to-end latency (event → submission) | <15 minutes |
| Availability | 99.9% |
| Submission acknowledgement | Required |

### Regulatory Reporter Failure Handling

| Failure | Behaviour | Alert |
|---------|-----------|-------|
| Regulator API down | Retry with exponential backoff (1s, 2s, 4s, 8s, 16s) | P1 after 3 failures |
| Retry exhaustion | Dead-letter to `control.regulatory_dead_letter`; manual review | P1 immediate |
| Submission timeout | Retry; check for duplicate via submission reference | P2 |
| Missing reference data | Reject event; alert for investigation | P2 |

### Idempotency

The reporter ensures idempotent submissions:
- Each submission includes `event_id` (deterministic) and `regulator_reference` (returned)
- `control.regulatory_submissions` has unique constraint on `event_id`
- Duplicate submissions are detected and logged, not resubmitted

### Cache Latency Trade-off

Reference data cache refreshes every 5 minutes from BigQuery snapshots. If a new trader ID is ingested by batch, the regulatory reporter won't see it until the next cache refresh. This is documented in the support runbook.

**Mitigation:** For urgent reference data changes, trigger manual cache refresh via `/admin/refresh-cache` endpoint.

---

## Audit & Compliance

This design follows the principle of least privilege. All data at rest is encrypted by default (BigQuery, GCS) and all communication between services occurs over GCP internal or TLS-secured network paths. Access to datasets and buckets is granted to service accounts with the minimum required permissions only.

### Control Tables

All control tables are partitioned by `DATE(run_timestamp)` and clustered by `source_name` or `model_name`:

| Table | Purpose |
|-------|---------|
| `control.validation_runs` | Every file validation attempt |
| `control.dbt_runs` | Every dbt model execution |
| `control.source_completeness` | Daily source arrival tracking |
| `control.ingested_files` | File-level audit trail |
| `control.streaming_sequence_gaps` | Missing message detection |
| `control.regulatory_submissions` | Regulatory submission audit trail |
| `control.regulatory_dead_letter` | Failed submissions for manual review |

### Data Retention

Data retention is aligned to regulatory and internal policy requirements:

- **Landing / failed buckets:** 90 days (sufficient for replay and investigation)
- **Archive buckets:** 7 years (regulatory-grade audit trail of source files)
- **BigQuery raw/staging tables:** 30 days (used for short-term recovery and backfills)
- **BigQuery curation/consumer tables:** 7 years (primary analytical and surveillance history)
- **Control and regulatory tables:** 7 years (end-to-end pipeline and submission audit trail)

These values can be adjusted via Terraform but any change requires architecture and compliance approval.

---

## Observability & Alerting

### Dynatrace Alerts

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Validation failed | `event=validation_failed` | P3 | Contact source owner |
| Source missing | `event=source_missing` | P3 | Check upstream |
| Volume anomaly (critical) | `z_score < -3 OR z_score > 3` | P2 | Investigate |
| dbt model failed | `event=dbt_model_failed` | P2 | Check logs |
| Pipeline stale | No run in 4 hours | P2 | Check pod |
| Streaming lag | `lag_minutes > 30` | P2 | Check Kafka/Pub/Sub |
| Sequence gaps | `gap_size > 100` | P2 | Investigate upstream |
| Regulatory late | `status = 'LATE'` | P1 | Immediate escalation |
| Regulatory reporter down | Health check fails | P1 | Immediate escalation |

---

## Support Runbook

See [`docs/support_runbook.md`](docs/support_runbook.md) for detailed procedures.

**Quick Reference:**

| Symptom | First Action |
|---------|--------------|
| "Source missing" alert | Check upstream system status; contact source owner |
| "Validation failed" alert | Check `failed/` bucket for file and error log |
| "dbt model failed" alert | Check `control.dbt_runs` for error message |
| "Regulatory late" alert | Check `control.regulatory_reconciliation`; escalate to on-call |
| Surveillance partner reports missing data | Check `control.source_completeness` for the date |

---

## Cost Model

| Component | Cost Driver | Estimate | Assumptions |
|-----------|-------------|----------|-------------|
| GCS | Storage + operations | ~£50/month | 500GB stored, 1M operations |
| BigQuery Storage | Data at rest | ~£200/month | 10TB total |
| BigQuery Compute | Queries (batch) | ~£300/month | Incremental processing, ~5TB scanned/day |
| BigQuery Streaming | Streaming inserts (~300GB/day) | ~£400/month | RFQ stream volume |
| GKE | Batch + streaming pods | ~£100/month | 3 nodes, e2-standard-4 |
| Cloud Run | Regulatory reporter | ~£50/month | ~100k requests/day |
| Pub/Sub | Message throughput | ~£100/month | ~300M messages/day |
| **Total** | | **~£1,200/month** | |

**Notes:**
- Costs scale with data volume
- Surveillance partner query costs are **excluded** (billed separately to their project)
- GKE sizing assumes 2 validator pods + 3 Kafka bridge pods

---

## FAQ

**Q: Why not use Airflow/Composer?**  
A: Our pipeline is essentially linear. Composer adds operational overhead for something that's just "validate, transform, archive."

**Q: Why not Spark/Dataflow?**  
A: BigQuery handles our scale. We're processing hundreds of millions of rows, not billions.

**Q: When should I use streaming vs batch?**  
A: Batch for hourly SLAs (simpler). Streaming for >50M messages/day or sub-hour latency. Real-time services only for regulatory deadlines.

**Q: Why bridge Kafka through Pub/Sub?**  
A: BigQuery's native Pub/Sub integration handles backpressure, retries, and dead-lettering automatically.

**Q: Why deterministic UUIDs instead of auto-increment?**  
A: Deterministic IDs enable idempotent processing — reprocessing produces identical trade IDs.

**Q: How do we handle regulatory reporting latency?**  
A: Dedicated streaming service runs parallel to batch. Enriches from cached reference data, submits within minutes.

**Q: How do we add a new source?**  
A: Add YAML to `source_specs/`, create dbt staging model. See [`docs/adding_new_source.md`](docs/adding_new_source.md).

**Q: What if reference data changes after the regulatory reporter has cached it?**  
A: Cache refreshes every 5 minutes. For urgent changes, use `/admin/refresh-cache`. This is a known trade-off documented in the runbook.

---

## Glossary

| Term | Definition |
|------|------------|
| **Batch** | File-based ingestion processed hourly |
| **Curation Layer** | dbt models that enrich and join data; where `trade_id` is created |
| **Control Table** | BigQuery tables tracking pipeline operations for audit |
| **Dead Letter** | Failed messages/submissions stored for manual review |
| **dbt** | Data Build Tool — SQL-based transformation framework |
| **Deterministic UUID** | UUID5 generated from fixed inputs; same input = same output |
| **External Table** | BigQuery table backed by GCS files (no data copy) |
| **Golden Source** | Authoritative upstream system for a data domain |
| **Incremental Model** | dbt model that processes only new/changed rows |
| **Landing** | GCS bucket where raw files arrive |
| **MAR** | Market Abuse Regulation — EU regulatory framework |
| **Namespace UUID** | Fixed UUID used as seed for deterministic ID generation |
| **SCD Type 1** | Slowly Changing Dimension — overwrites with current value |
| **SCD Type 2** | Slowly Changing Dimension — preserves full history |
| **Snapshot** | dbt feature for SCD Type 2 dimensions |
| **Source Spec** | YAML file defining schema and validation rules for a source |
| **Staging** | GCS bucket for validated files; also dbt layer for cleaned data |
| **Streaming Insert** | Real-time row insertion to BigQuery via Pub/Sub |
| **Surveillance Extract** | Daily JSON file sent to surveillance partner |
| **UUID5** | Deterministic UUID algorithm using SHA-1 hash |

---

## Next Steps

1. Review architecture with team
2. Set up dev GCP project
3. Implement validator with first file-based source
4. Build dbt models for staging layer
5. Deploy batch pipeline to GKE
6. Add streaming bridge for high-volume Kafka source
7. Implement regulatory reporter service
8. Add Dynatrace integration
9. Onboard production sources
10. Iterate