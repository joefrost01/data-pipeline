# Markets Data Pipeline

A modern data platform for Markets using BigQuery, dbt, and minimal orchestration.

## Quick Start

```bash
# 1. Set up Python environment
python -m venv .venv
source .venv/bin/activate
pip install -e orchestrator/

# 2. Install dbt dependencies
cd dbt_project
dbt deps

# 3. Deploy infrastructure (int environment)
cd ../terraform/int
terraform init
terraform apply
```

## Project Structure

```
data-pipeline/
├── dbt_project/        # SQL transformations (dbt)
├── orchestrator/       # Python pipeline orchestration
├── source_specs/       # YAML validation specs per source
├── terraform/          # Infrastructure as code
│   └── int/            # Integration environment
└── DESIGN.md           # Architecture & design decisions
```

## Key Concepts

- **Deterministic identity** — `trade_id` is generated from source identifiers, enabling idempotent reprocessing
- **Three lanes** — Batch (hourly), Streaming (sub-hour), Regulatory (real-time) — operationally independent
- **SQL everywhere** — All business logic lives in dbt models
- **Configuration over code** — Add sources via YAML, not code changes

## Adding a New Source

1. Create `source_specs/{domain}/{source_name}.yaml`
2. Create `dbt_project/models/staging/stg_{source_name}.sql`
3. Add to `dbt_project/models/staging/sources.yml`
4. Run `make test-source SOURCE={source_name}`

See `source_specs/trading/murex_trades.yaml` for a complete example.

## Documentation

- [docs/design.md](docs/design.md) — Full architecture, design decisions, failure modes
- [docs/support_runbook.md](docs/support_runbook.md) — Operational procedures
- [docs/adding_new_source.md](docs/adding_new_source.md) — Detailed onboarding guide