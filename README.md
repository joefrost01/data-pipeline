# Data Pipeline Fixes

This directory contains all the fixed files from the code review. Copy these files into your repository to apply the fixes.

## How to Apply

```bash
# From your repository root:
cp -r data-pipeline-fixes/* .
```

Or selectively copy specific fixes.

---

## Critical Fixes

### 1. Race Condition in Archiver (Critical)

**Files:**
- `orchestrator/orchestrator/archiver.py` - Complete rewrite
- `orchestrator/orchestrator/validator.py` - Added `validated_output_paths` to result
- `orchestrator/orchestrator/main.py` - Updated to pass paths to archiver

**Problem:** The archiver queried BigQuery for files validated "before run_start_time", but the validator writes records during the same run, causing some files to be missed or incorrectly archived.

**Solution:** Pass the set of validated output paths directly from the validator to the archiver, eliminating the database query entirely.

### 2. MD5 vs UUID5 Documentation Mismatch (Critical)

**Files:**
- `docs/design_identity_section_fix.md` - Replacement text for design.md

**Problem:** Documentation mentions UUID5 but implementation uses MD5.

**Solution:** Updated documentation to accurately describe MD5 usage with clear notes about Python compatibility.

### 3. Missing stg_venue_a_trades Model (Critical)

**Files:**
- `dbt_project/models/staging/stg_venue_a_trades.sql` - New file

**Problem:** `trades_enriched.sql` references this model but it didn't exist.

**Solution:** Created the staging model (note: the file was in the documents but listed as a separate document - this ensures it's in the right location).

### 4. Duplicate schema.yml Content (Critical)

**Files:**
- `dbt_project/models/staging/schema.yml` - Complete rewrite

**Problem:** staging/schema.yml contained dim_instrument documentation that belongs in dimensions/schema.yml.

**Solution:** Removed duplicate content and added proper documentation for all staging models.

---

## High Priority Fixes

### 5. Counterparty Cache Duplicate Issue

**Files:**
- `regulatory_reporter/main.py` - Complete rewrite

**Problem:** Counterparty cache used single dict for both ID and name lookup, causing silent overwrites when names collide.

**Solution:** Separate `counterparties_by_id` and `counterparties_by_name` dictionaries.

### 6 & 9. Event ID Generation Mismatch

**Files:**
- `regulatory_reporter/main.py` - Fixed `_generate_event_id` method

**Problem:** Python event ID generation didn't include namespace or "event" prefix, unlike the dbt macro.

**Solution:** Updated to match dbt format: `{namespace}:event:{domain}:{source_system}:{source_event_id}`

### 7. Missing Staging Models for Snapshots

**Files:**
- `dbt_project/models/staging/stg_counterparties.sql` - New file
- `dbt_project/models/staging/stg_books.sql` - New file
- `dbt_project/snapshots/counterparties_snapshot.sql` - Updated to use staging
- `dbt_project/snapshots/books_snapshot.sql` - Updated to use staging

**Problem:** Snapshots referenced non-existent staging models.

**Solution:** Created the missing staging models and updated snapshots to reference them.

### 8. Missing Orchestrator BigQuery IAM

**Files:**
- `terraform/int/iam.tf` - Added BigQuery permissions

**Problem:** Orchestrator service account lacked BigQuery permissions.

**Solution:** Added `bigquery.jobUser` and dataset-level `dataEditor`/`dataViewer` roles for all service accounts.

---

## Medium Priority Fixes

### 10. Duplicate Streaming Bridge

**Files:**
- `streaming/bridge.py` - Now re-exports from orchestrator

**Problem:** Same code existed in two places.

**Solution:** `streaming/bridge.py` now imports from `orchestrator.bridge` instead of duplicating code.

### 12. Invalid expected_sources.csv

**Files:**
- `dbt_project/seeds/expected_sources.csv` - Removed non-existent sources

**Problem:** Referenced sources (venue_b_trades, bloomberg_rfqs) that don't exist.

**Solution:** Removed non-existent sources; add them back when implemented.

### 13. XML Namespace Handling

**Files:**
- `orchestrator/orchestrator/parsers.py` - Fixed XML parser

**Problem:** XML parser didn't handle namespaced elements like `mx:Trade`.

**Solution:** Added `_build_tag_matcher` and `_element_matches` methods to properly handle Clark notation namespaces.

### 14. Control File Path Bug

**Files:**
- `orchestrator/orchestrator/validator.py` - Fixed `_get_expected_row_count`

**Problem:** Control file lookup didn't preserve directory path.

**Solution:** Now correctly constructs control file path including parent directory.

### 17. force_full_refresh Variable

**Files:**
- `dbt_project/models/curation/trades_enriched.sql` - Fixed variable usage

**Problem:** `full_refresh` config option doesn't work at runtime as intended.

**Solution:** Use the variable in the incremental filter logic instead: `{% if is_incremental() and not do_full_refresh %}`

---

## Low Priority Fixes

### 19. trade_model_v2.md Status

**Files:**
- `docs/trade_model_v2_header.md` - Header to prepend to existing doc

**Problem:** Document describes future state but wasn't marked as such.

**Solution:** Added clear status note explaining this is a future design, not current implementation.

---

## New Files Added

### CI Workflow
- `.github/workflows/ci.yml` - Complete CI pipeline including namespace check

### Makefile
- `Makefile` - Common operations referenced in docs

---

## Files Summary

```
data-pipeline-fixes/
├── .github/
│   └── workflows/
│       └── ci.yml                          # NEW: CI pipeline
├── dbt_project/
│   ├── models/
│   │   ├── curation/
│   │   │   └── trades_enriched.sql         # FIXED: force_full_refresh logic
│   │   └── staging/
│   │       ├── schema.yml                  # FIXED: removed duplicate content
│   │       ├── stg_books.sql               # NEW: missing staging model
│   │       ├── stg_counterparties.sql      # NEW: missing staging model
│   │       └── stg_venue_a_trades.sql      # NEW: missing staging model
│   ├── seeds/
│   │   └── expected_sources.csv            # FIXED: removed non-existent sources
│   └── snapshots/
│       ├── books_snapshot.sql              # FIXED: reference staging model
│       └── counterparties_snapshot.sql     # FIXED: reference staging model
├── docs/
│   ├── design_identity_section_fix.md      # REPLACEMENT: for design.md sections
│   └── trade_model_v2_header.md            # PREPEND: to trade_model_v2.md
├── orchestrator/
│   └── orchestrator/
│       ├── archiver.py                     # FIXED: race condition
│       ├── main.py                         # FIXED: pass validated paths
│       ├── parsers.py                      # FIXED: XML namespace handling
│       └── validator.py                    # FIXED: return validated paths, GCS retry
├── regulatory_reporter/
│   └── main.py                             # FIXED: cache & event ID
├── streaming/
│   └── bridge.py                           # FIXED: import from orchestrator
├── terraform/
│   └── int/
│       └── iam.tf                          # FIXED: BigQuery permissions
├── Makefile                                # NEW: common operations
└── README.md                               # This file
```

---

## Testing After Applying

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
make integration-test
```
