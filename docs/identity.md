# Deterministic Identity

How we generate stable, reproducible trade identifiers across the pipeline.

## The Problem

Trade data arrives from multiple source systems, each with their own identifiers:

- Murex: `MX-2025-001234`
- Venue A: `EXEC-98765`
- Venue B: `VB_T_20250115_0001`

We need a single `trade_id` that:

1. Is unique across all sources
2. Is stable — same input always produces same output
3. Enables idempotent reprocessing
4. Requires no coordination between processes
5. Is compact enough to join efficiently

## The Solution

We generate a deterministic identifier using an MD5 hash of the source identity:

```
trade_id = MD5(namespace:domain:source_system:source_trade_id)
```

This produces a 32-character hex string that is consistent across runs.

> **Note:** This is MD5, not UUID5. We use MD5 for simplicity and performance in BigQuery. 
> UUID5 uses SHA-1 with specific byte formatting — if you need true UUID5 for cross-system 
> compatibility (e.g., with external systems using Python's `uuid.uuid5()`), the outputs 
> will differ. For internal use within this pipeline, MD5 provides identical deterministic 
> properties.

### Implementation

The `generate_trade_id` macro in dbt:

```sql
{{ generate_trade_id("'markets'", "source_system", "source_trade_id") }}

-- Expands to:
to_hex(
    md5(
        concat(
            'a1b2c3d4-e5f6-7890-abcd-ef1234567890',  -- namespace
            ':',
            'markets',                                -- domain
            ':',
            source_system,                            -- e.g., 'MUREX'
            ':',
            cast(source_trade_id as string)           -- e.g., 'MX-2025-001234'
        )
    )
)
```

**Example outputs:**

| Source System | Source Trade ID | Generated trade_id |
|---------------|-----------------|-------------------|
| MUREX | MX-2025-001234 | `7a3f2b1c9d8e4f5a...` |
| VENUE_A | EXEC-98765 | `2b4c6d8e0f1a3b5c...` |
| MUREX | MX-2025-001234 | `7a3f2b1c9d8e4f5a...` ← Same input = same output |

## Why This Works

### Idempotent Reprocessing

If Murex resends yesterday's file, we regenerate the same `trade_id` values. The incremental merge in `trades_enriched` uses `trade_id` as the unique key, so:

- New trades → inserted
- Reprocessed trades → overwritten (no duplicates)
- Corrected trades → overwritten with new values

No special handling, no correction flags, no amendment chains.

### No Coordination

Each dbt model, each parallel process, each environment generates identical IDs for identical inputs. There's no sequence to maintain, no central ID service to call, no race conditions.

### Cross-System Joins

The surveillance partner can join our extract back to their own data using `trade_id`. If they reprocess, their joins still work because our IDs haven't changed.

## The Namespace

The namespace UUID (`a1b2c3d4-e5f6-7890-abcd-ef1234567890`) is critical:

- It's defined once in `dbt_project.yml` as `markets_namespace`
- It must never change in production
- It prevents collisions with other systems using the same approach

**If the namespace changes, all trade_ids change.** This breaks:

- Incremental models (everything looks like new rows)
- Historical joins
- Partner reconciliation

### Protecting the Namespace

The namespace is validated in CI:

```yaml
# .github/workflows/ci.yml
- name: Check namespace unchanged
  run: |
    CURRENT=$(grep 'markets_namespace' dbt_project/dbt_project.yml | cut -d"'" -f2)
    EXPECTED="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    if [ "$CURRENT" != "$EXPECTED" ]; then
      echo "ERROR: Namespace UUID has changed!"
      exit 1
    fi
```

## Components

The identity string has four parts:

```
{namespace}:{domain}:{source_system}:{source_trade_id}
```

| Component | Purpose | Example |
|-----------|---------|---------|
| `namespace` | Prevents global collisions | `a1b2c3d4-...` |
| `domain` | Allows multiple domains in same pipeline | `markets`, `treasury` |
| `source_system` | Distinguishes sources with overlapping IDs | `MUREX`, `VENUE_A` |
| `source_trade_id` | Native identifier from source | `MX-2025-001234` |

### Why Include Domain?

If Treasury later onboards to the same pipeline, a Murex trade `MX-2025-001234` in Markets is different from `MX-2025-001234` in Treasury. The domain component keeps them distinct.

### Why Include Source System?

Two venues might both use sequential IDs starting at 1. Without `source_system`, trade `1` from Venue A would collide with trade `1` from Venue B.

## Edge Cases

### Composite Source IDs

Some sources have composite keys:

```yaml
# source_spec
processing:
  identity:
    source_id_field: "exec_id || '-' || exec_leg"
```

In the staging model:

```sql
concat(exec_id, '-', cast(exec_leg as string)) as source_trade_id
```

### Amendments and Versions

For sources that send amendments with version numbers:

```sql
-- In staging, we keep the base trade_id stable
exec_id as source_trade_id,  -- NOT exec_id + version
version as amendment_version

-- In curation, we dedupe to latest version
qualify row_number() over (
    partition by trade_id
    order by amendment_version desc
) = 1
```

The `trade_id` stays the same across versions — that's the point. We want v1, v2, v3 of a trade to resolve to one row with the latest values.

### Source ID Changes

If a source system changes their ID format (e.g., `MX-001234` → `MUREX-001234`), this creates new trade_ids. Options:

1. **Mapping table** — translate new format to old in staging
2. **Accept the break** — if old data is archived and won't rejoin
3. **Backfill** — regenerate historical data with new format

Option 1 is usually right for actively used data.

## Why Not UUID4?

Random UUIDs (v4) would be unique, but:

- Not deterministic — reprocessing creates duplicates
- Requires storing generated IDs somewhere
- Can't regenerate from source data alone

## Why Not Sequence?

Auto-incrementing sequences:

- Require coordination (database sequence, distributed ID service)
- Create ordering dependencies
- Make parallel processing harder
- Can't regenerate from source data

## Why Not Hash the Whole Row?

Hashing all fields means any field change creates a new ID:

- Price correction → new trade_id
- Counterparty name update → new trade_id
- Late enrichment → new trade_id

We want the ID to be stable for the *identity* of the trade (what trade is this?), not its *attributes* (what are the current values?).

## Testing Identity

### Unit Test: Determinism

```sql
-- tests/test_trade_id_deterministic.sql
with test_cases as (
    select
        {{ generate_trade_id("'markets'", "'MUREX'", "'TEST-001'") }} as id1,
        {{ generate_trade_id("'markets'", "'MUREX'", "'TEST-001'") }} as id2
)
select * from test_cases where id1 != id2
-- Should return 0 rows
```

### Unit Test: Uniqueness

```sql
-- tests/test_trade_id_unique_across_sources.sql
with test_cases as (
    select
        {{ generate_trade_id("'markets'", "'MUREX'", "'001'") }} as murex_id,
        {{ generate_trade_id("'markets'", "'VENUE_A'", "'001'") }} as venue_a_id
)
select * from test_cases where murex_id = venue_a_id
-- Should return 0 rows
```

### Integration Test: Reprocessing

```bash
# Drop same file twice, verify no duplicates
gsutil cp test_file.csv gs://landing/murex/
kubectl create job --from=cronjob/pipeline run-1 -n markets
# Wait for completion
gsutil cp test_file.csv gs://landing/murex/
kubectl create job --from=cronjob/pipeline run-2 -n markets
# Wait for completion

# Verify count unchanged
bq query "SELECT COUNT(*) FROM curation.trades_enriched WHERE source_system = 'TEST'"
```

## Python Compatibility Note

If you need to generate the same IDs in Python (e.g., for testing or the regulatory reporter), use:

```python
import hashlib

MARKETS_NAMESPACE = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

def generate_trade_id(domain: str, source_system: str, source_trade_id: str) -> str:
    """Generate deterministic trade ID matching the dbt macro."""
    id_string = f"{MARKETS_NAMESPACE}:{domain}:{source_system}:{source_trade_id}"
    return hashlib.md5(id_string.encode()).hexdigest()

# Example
trade_id = generate_trade_id("markets", "MUREX", "TRD-12345")
# Returns same value as the dbt macro
```

> **Important:** This differs from `uuid.uuid5()`. If you have external systems using 
> UUID5, you'll need a mapping layer or to align on one approach.

## Summary

Deterministic identity is the foundation that makes everything else simple:

- Reprocessing just works
- Corrections just work
- Deduplication just works
- Cross-system joins just work

One decision — hash the source identity — and the hard problems solve themselves.