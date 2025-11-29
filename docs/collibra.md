# Collibra Integration

How the Markets Data Pipeline integrates with Collibra for data governance, lineage, and cataloguing.

## Overview

This pipeline integrates with Collibra as a **consumer of governance metadata** and a **producer of technical lineage**. We don't duplicate Collibra's role as the catalogue of record — we reference it and feed it.

## Integration Points

### 1. Asset Registration (Pipeline → Collibra)

Register pipeline assets in Collibra's Data Catalog:

| Asset Type | Collibra Domain | Registration Method |
|------------|-----------------|---------------------|
| Source specs | Data Sources | API sync from `source_specs/*.yaml` |
| BigQuery datasets | Physical Data Assets | Automatic via Collibra GCP connector |
| dbt models | Data Assets | dbt docs → Collibra import |
| Extract files | Data Products | API registration on extract generation |

**Recommended approach:** Use Collibra's REST API to sync source specs on CI/CD merge to main. This keeps the catalogue aligned with what's actually deployed.
```python
# Example: Register source spec in Collibra
def register_source_in_collibra(spec: dict) -> None:
    collibra_client.post("/assets", json={
        "name": spec["name"],
        "domainId": COLLIBRA_DATA_SOURCES_DOMAIN,
        "typeId": COLLIBRA_DATA_SOURCE_TYPE,
        "attributes": {
            "Description": spec["description"],
            "Owner": spec["owner"],
            "Expected Frequency": spec["expectations"]["frequency"],
            "Format": spec["source"]["format"],
        }
    })
```

### 2. Lineage (Pipeline → Collibra)

Collibra needs to understand: *where does the surveillance extract come from?*

**Option A: dbt artifacts (recommended)**

dbt generates `manifest.json` and `catalog.json` containing full model lineage. Collibra can ingest these directly:
```bash
# After dbt build, push artifacts to Collibra
dbt docs generate
python scripts/push_lineage_to_collibra.py \
    --manifest target/manifest.json \
    --catalog target/catalog.json
```

**Option B: Collibra's BigQuery scanner**

Collibra's GCP integration can scan BigQuery `INFORMATION_SCHEMA` for table-level lineage. This captures the physical lineage but misses the logical model relationships.

**Recommendation:** Use both. Let Collibra scan BigQuery for physical assets, then overlay dbt lineage for the transformation logic.

### 3. Business Glossary (Collibra → Pipeline)

Reference Collibra's business glossary for canonical definitions:

| Pipeline Concept | Collibra Business Term |
|------------------|------------------------|
| `trade_id` | "Trade Identifier" |
| `counterparty_lei` | "Legal Entity Identifier" |
| `notional` | "Trade Notional Value" |

**Implementation:** Add Collibra term references to dbt model descriptions:
```yaml
# schema.yml
models:
  - name: trades_enriched
    description: |
      Enriched trade records from all sources.
      See Collibra: https://collibra.company.com/asset/12345
    columns:
      - name: trade_id
        description: >
          Deterministic trade identifier. 
          Business term: https://collibra.company.com/term/trade-identifier
```

This keeps dbt as the source of technical documentation while linking to Collibra for business context.

### 4. Data Quality Rules (Collibra ↔ Pipeline)

Two options for aligning DQ rules:

**Option A: Collibra as source of truth**

Export DQ rules from Collibra, generate dbt tests:
```python
# scripts/sync_dq_from_collibra.py
rules = collibra_client.get(f"/assets/{asset_id}/rules")
for rule in rules:
    generate_dbt_test(rule)  # Creates tests/collibra_rule_{id}.sql
```

**Option B: dbt as source of truth (recommended for this pipeline)**

Our validation rules live in source specs and dbt tests. Push results to Collibra:
```python
# After dbt test, report results
def report_dq_to_collibra(test_results: list[dict]) -> None:
    for result in test_results:
        collibra_client.post("/dataQualityResults", json={
            "assetId": get_collibra_asset_id(result["model"]),
            "ruleId": get_collibra_rule_id(result["test"]),
            "passed": result["status"] == "pass",
            "executedAt": result["timestamp"],
            "rowsTested": result.get("rows_tested"),
            "rowsFailed": result.get("failures"),
        })
```

**Recommendation:** Keep rules in dbt (version controlled, tested in CI), push results to Collibra for visibility.

### 5. Access Governance (Collibra → Pipeline)

Collibra can manage data access policies. For this pipeline:

- **Dataset-level access** is managed in Terraform (IAM bindings)
- **Column-level access** could use BigQuery column-level security with policies defined in Collibra

For now, keep access control in Terraform. If fine-grained access becomes necessary, implement a sync from Collibra policies to BigQuery column ACLs.

## Integration Architecture
```
┌─────────────────────────────────────────────────────────────────────┐
│                            Collibra                                 │
│                                                                     │
│   Business Glossary ◄──────────────────────── dbt docs reference    │
│                                                                     │
│   Data Catalog ◄─────────────────────────────── Source specs sync   │
│        │                                              │             │
│        ▼                                              │             │
│   Technical Assets ◄── BigQuery scanner ◄─── BigQuery datasets      │
│        │                                              │             │
│        ▼                                              │             │
│   Lineage Graph ◄───────────────────────────── dbt manifest.json    │
│                                                       │             │
│   DQ Dashboard ◄──────────────────────────────── dbt test results   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Implementation Approach

### Phase 1: Visibility (Week 1-2)

1. Enable Collibra's BigQuery connector to scan datasets
2. Push source specs to Collibra on merge to main
3. Add Collibra term links to key dbt model descriptions

### Phase 2: Lineage (Week 3-4)

1. Create script to transform dbt manifest → Collibra lineage format
2. Run after each production dbt build
3. Verify lineage appears correctly in Collibra

### Phase 3: Quality (Week 5-6)

1. Map dbt tests to Collibra DQ rules
2. Push test results after each pipeline run
3. Configure Collibra dashboards for DQ visibility

## What We Don't Do

| Anti-pattern | Why we avoid it |
|--------------|-----------------|
| Manage schemas in Collibra | dbt is the source of truth for physical schemas |
| Run DQ rules from Collibra | Rules execute in dbt; Collibra shows results |
| Generate code from Collibra | Configuration lives in Git, not Collibra |
| Duplicate documentation | dbt docs for technical, Collibra links for business |

Collibra is the **catalogue and governance layer**, not the execution engine. The pipeline pushes metadata up; it doesn't pull configuration down.

## Configuration
```yaml
# config/collibra.yaml
collibra:
  base_url: https://company.collibra.com/rest/2.0
  
  # Domain IDs for asset registration
  domains:
    data_sources: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    physical_assets: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    data_products: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  
  # Asset type IDs
  types:
    source_system: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    bigquery_table: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    data_extract: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
  
  # Sync settings
  sync:
    on_merge: true
    on_dbt_build: true
    include_test_results: true
```

Authentication via service account with Collibra API credentials stored in Secret Manager.

## Scripts

| Script | Trigger | Purpose |
|--------|---------|---------|
| `scripts/collibra_sync_sources.py` | CI merge to main | Register/update source specs |
| `scripts/collibra_push_lineage.py` | After dbt build | Push dbt lineage graph |
| `scripts/collibra_push_dq.py` | After dbt test | Push test results |

## Summary

The integration is lightweight and unidirectional for the most part:

- **Pipeline → Collibra:** Asset registration, lineage, DQ results
- **Collibra → Pipeline:** Business glossary links (documentation only)

This keeps the pipeline self-contained and version-controlled while giving Collibra the visibility it needs for governance. We don't create a dependency on Collibra for pipeline operation — if Collibra is down, the pipeline still runs.