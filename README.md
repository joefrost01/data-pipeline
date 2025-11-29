# Markets Data Pipeline

A modern, lightweight data platform for Markets using BigQuery, dbt, and minimal orchestration. Designed for simplicity, auditability, and operational excellence.

## Overview

This pipeline ingests trade data from multiple upstream sources (venues, OMS, Bloomberg, Kafka streams, etc.), validates and transforms it through a series of layers, and produces daily extracts for a third-party Trade Surveillance partner.

### Key Features

- **Single repository** containing all pipeline code, tests, documentation, and configuration
- **Hourly batch processing** with incremental models — always up to date
- **Streaming capability** for high-volume or low-latency sources
- **Full audit trail** of every file and message received, validated, and processed
- **Self-healing pipeline** — drop a file in landing, it gets processed
- **Deterministic identity** — globally unique, reproducible trade IDs using MD5

## Quick Start

```bash
# Install dependencies
make install

# Run linting
make lint

# Run Python tests
make test

# Compile dbt to check for errors
make dbt-compile

# Validate source specs
make validate-specs

# Smoke test a source (requires GCP access)
make test-source SOURCE=murex_trades PROJECT_ID=markets-int-12345
```

## Project Structure

```
markets-pipeline/
├── dbt_project/           # dbt models, macros, tests
│   ├── models/
│   │   ├── staging/       # Clean, typed source data
│   │   ├── curation/      # Enriched, joined data (trade_id created here)
│   │   ├── consumer/      # Mart tables for consumption (markets_extract)
│   │   ├── dimensions/    # Reference dimensions
│   │   └── control/       # Pipeline control models
│   ├── snapshots/         # SCD Type 2 dimensions
│   ├── macros/            # Including generate_trade_id
│   └── seeds/             # Static reference data
├── orchestrator/          # Python orchestration code
│   ├── orchestrator/      # Main package
│   └── k8s/               # Kubernetes manifests
├── regulatory_reporter/   # Low-latency reporting service
├── streaming/             # Kafka bridge components
├── source_specs/          # YAML source definitions
├── terraform/             # Infrastructure as code
├── scripts/               # Utility scripts
└── docs/                  # Documentation
```

## Documentation

- [Design Document](docs/design.md) — Architecture overview
- [Adding a New Source](docs/adding_new_source.md) — Step-by-step guide
- [Identity Management](docs/identity.md) — Trade ID generation
- [Streaming Architecture](docs/streaming.md) — Kafka integration
- [Support Runbook](docs/support_runbook.md) — Operational procedures
- [Testing Guide](docs/testing.md) — How to test the pipeline
- [Environment Promotion](docs/environment_promotion.md) — Deployment workflow

## Architecture

The platform comprises three loosely coupled subsystems:

```
LANE 1: BATCH TRADE PROCESSING (Core)
  Source Files → GCS Landing → Validator → GCS Staging → BigQuery (dbt) → Extract

LANE 2: HIGH-VOLUME STREAMING INGESTION
  Kafka → Pub/Sub Bridge → BigQuery Streaming Insert → dbt (hourly merge)

LANE 3: LOW-LATENCY REGULATORY REPORTING
  Kafka/Pub/Sub → Regulatory Reporter (Cloud Run) → Regulator API
```

## Key Design Decisions

1. **MD5 for Trade IDs** — Deterministic, BigQuery-native, no coordination required
2. **SQL for all transformations** — All business rules in dbt, not Python
3. **Configuration over code** — New sources require only a YAML file
4. **Self-healing by default** — 7-day window provides resilience for late files

## Environments

| Environment | Project ID | Purpose |
|-------------|------------|---------|
| int | markets-int-12345 | Integration testing, UAT |
| prod | markets-prod-12345 | Production |

## CI/CD

The pipeline uses GitHub Actions for CI:

- Lint checks (ruff, mypy)
- Python unit tests
- dbt compile validation
- Source spec validation (including XPath syntax)
- **Namespace protection** — CI fails if the trade ID namespace changes

## Testing After Changes

```bash
# 1. Run linting
make lint

# 2. Run Python tests
make test

# 3. Compile dbt to check for errors
make dbt-compile

# 4. Validate source specs
make validate-specs

# 5. Run the full CI locally (if you have GCP access)
make integration-test PROJECT_ID=markets-int-12345
```

## Important Notes

### Naming Convention

This pipeline uses the `markets` prefix throughout (buckets, metrics, K8s resources). 
The consumer model is `markets_extract` in the `consumer` schema.

### Namespace UUID

The `markets_namespace` in `dbt_project.yml` is **immutable**. Changing it will invalidate
all existing trade IDs. This is protected by CI validation.

### Extract Table

The extract is generated from `consumer.markets_extract`. This can be overridden via
the `EXTRACT_TABLE` environment variable if needed.
