# Surveillance Infrastructure

Terraform configuration for the surveillance data platform.

## Resources

| Resource | Purpose |
|----------|---------|
| **GCS Buckets** | landing, archive, failed, staging |
| **BigQuery Datasets** | surveillance_raw, surveillance_staging, event_interaction |
| **BigQuery Tables** | raw_futures_order_events, order_financial_markets |
| **Service Account** | Orchestrator with Workload Identity |

## BigQuery Structure

```
surveillance_raw
├── load_metadata
└── raw_futures_order_events (all strings)

surveillance_staging (views)
├── stg_load_metadata
└── stg_futures_order_events (typed)

event_interaction (curated)
├── order_financial_markets (OBT with nested structs)
└── order_financial_markets_current (view)
```

## Usage

```bash
# Initialise
terraform init

# Plan
terraform plan -var="project_id=your-project"

# Apply
terraform apply -var="project_id=your-project"
```

## Post-Apply

After Terraform creates the infrastructure:

1. Build and deploy the orchestrator container
2. Configure table mappings in the ConfigMap
3. Run dbt to populate curated tables

## Curated Table Schema

The `order_financial_markets` table uses nested structs:

```
├── Type: "Master.Transaction"
├── ExtractDate
├── Execution STRUCT
│   ├── Economics STRUCT
│   ├── Identifier STRUCT
│   └── Events ARRAY<STRUCT>
├── Unit STRUCT
└── Bi-temporal fields
```

All tables are Terraform-managed. dbt inserts into existing tables.
