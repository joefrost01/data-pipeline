# Testing Guide

How to test the Markets Data Pipeline at every level.

## Quick Reference

```bash
# Run all dbt tests
cd dbt_project && dbt test

# Test a single model
dbt test --select stg_murex_trades

# Validate a source spec
make validate-spec SOURCE=murex_trades

# Smoke test a source end-to-end
make test-source SOURCE=murex_trades

# Run Python unit tests
cd orchestrator && pytest

# Full integration test
make integration-test
```

---

## Testing Layers

| Layer | What | Tool | When |
|-------|------|------|------|
| Source spec | YAML validity, schema types | `validate-spec` | Before PR |
| Staging models | Schema tests, accepted values | dbt test | CI |
| Curation models | Uniqueness, relationships | dbt test | CI |
| Macros | Determinism, edge cases | dbt unit tests | CI |
| Orchestrator | Python logic | pytest | CI |
| End-to-end | Full pipeline flow | `test-source` | Before prod deploy |

---

## dbt Tests

### Running Tests

```bash
cd dbt_project

# All tests
dbt test

# Tests for one model
dbt test --select stg_murex_trades

# Tests for a model and its dependencies
dbt test --select +trades_enriched

# Only failed tests from last run
dbt test --select result:fail
```

### Schema Tests

Defined in `schema.yml` files:

```yaml
models:
  - name: stg_murex_trades
    columns:
      - name: source_trade_id
        tests:
          - not_null
          - unique
      - name: side
        tests:
          - accepted_values:
              values: ['BUY', 'SELL']
      - name: quantity
        tests:
          - dbt_utils.expression_is_true:
              expression: "> 0"
```

### Custom Tests

For complex validation, create SQL tests in `tests/`:

```sql
-- tests/assert_no_future_trades.sql
-- Fails if any trades have timestamps in the future

select *
from {{ ref('trades_enriched') }}
where trade_time > current_timestamp()
```

```sql
-- tests/assert_trade_id_deterministic.sql
-- Fails if regenerating trade_id produces different values

with regenerated as (
    select
        trade_id as original_id,
        {{ generate_trade_id("'markets'", "source_system", "source_trade_id") }} as regenerated_id
    from {{ ref('trades_enriched') }}
    where trade_date = current_date()
)
select * from regenerated
where original_id != regenerated_id
```

### Relationship Tests

Verify referential integrity:

```yaml
models:
  - name: trades_enriched
    columns:
      - name: trader_id
        tests:
          - relationships:
              to: ref('traders_snapshot')
              field: trader_id
              where: "dbt_valid_to is null"  # Current records only
```

### Freshness Tests

Verify sources are arriving on time:

```yaml
sources:
  - name: raw
    freshness:
      warn_after: {count: 2, period: hour}
      error_after: {count: 6, period: hour}
    loaded_at_field: _ingestion_time
    tables:
      - name: murex_trades
      - name: rfq_stream
```

Run with:
```bash
dbt source freshness
```

---

## Source Spec Validation

Before a source spec goes into production:

```bash
make validate-spec SOURCE=venue_b_trades
```

This checks:

1. **YAML validity** — Parses without error
2. **Required fields** — `name`, `source`, `schema` present
3. **Schema types** — All types are valid BigQuery types
4. **XPath validity** — For XML sources, xpaths are syntactically correct
5. **Constraint consistency** — `min_value` only on numeric types, etc.

### Manual Validation

```bash
cd orchestrator
python -m orchestrator.validate_spec ../source_specs/trading/venue_b_trades.yaml
```

Output:
```
✓ YAML parses correctly
✓ Required fields present
✓ Schema types valid
✓ Constraints consistent
✓ XPath syntax valid (XML only)

Source spec is valid.
```

---

## Smoke Testing a Source

End-to-end test with sample data:

```bash
make test-source SOURCE=murex_trades
```

This:

1. Creates a sample file matching the source spec
2. Uploads to a test prefix in landing bucket
3. Runs one pipeline cycle
4. Verifies rows appear in staging
5. Verifies rows appear in curation (with correct trade_id)
6. Cleans up test data

### Manual Smoke Test

```bash
# 1. Create sample file
cat > /tmp/test_trades.csv << 'EOF'
trade_id,trade_time,counterparty,trader_id,instrument,side,quantity,price,book_id
TEST-001,2025-01-15T10:30:00Z,ACME Corp,TRD001,AAPL,BUY,100,150.50,BOOK01
TEST-002,2025-01-15T10:31:00Z,Globex,TRD002,MSFT,SELL,200,380.25,BOOK02
EOF

# 2. Upload to landing
gsutil cp /tmp/test_trades.csv gs://markets-int-landing/murex/trades_test_20250115.csv

# 3. Run pipeline
kubectl create job --from=cronjob/markets-pipeline smoke-test-$(date +%s) -n markets

# 4. Wait and verify
bq query --use_legacy_sql=false '
  SELECT source_trade_id, trade_id, source_system
  FROM staging.stg_murex_trades
  WHERE source_trade_id LIKE "TEST-%"
'

# 5. Clean up
bq query --use_legacy_sql=false '
  DELETE FROM curation.trades_enriched
  WHERE source_trade_id LIKE "TEST-%"
'
gsutil rm gs://markets-int-archive/**/trades_test_*.parquet
```

---

## Orchestrator Tests

Python unit tests for orchestrator logic:

```bash
cd orchestrator
pip install -e ".[dev]"
pytest
```

### Test Structure

```
orchestrator/
├── tests/
│   ├── test_validator.py      # File validation logic
│   ├── test_parsers.py        # CSV, JSON, XML parsing
│   ├── test_config.py         # Config loading
│   └── test_control.py        # Control table writes
```

### Example Test

```python
# tests/test_parsers.py

def test_csv_parser_validates_required_fields():
    spec = {
        "schema": [
            {"name": "id", "type": "STRING", "nullable": False},
            {"name": "value", "type": "INT64", "nullable": True},
        ]
    }
    parser = CsvParser()
    
    valid_rows, quarantined = parser.parse_and_validate(
        "tests/fixtures/sample.csv", spec
    )
    
    assert len(valid_rows) == 2
    assert len(quarantined) == 1
    assert "required field" in quarantined[0]["failure_reason"].lower()


def test_csv_parser_converts_types():
    spec = {
        "schema": [
            {"name": "quantity", "type": "INT64", "nullable": False},
            {"name": "price", "type": "NUMERIC", "nullable": False},
        ]
    }
    parser = CsvParser()
    
    valid_rows, _ = parser.parse_and_validate(
        "tests/fixtures/typed.csv", spec
    )
    
    assert valid_rows[0]["quantity"] == 100
    assert valid_rows[0]["price"] == Decimal("150.50")
```

### Running Specific Tests

```bash
# One test file
pytest tests/test_parsers.py

# One test function
pytest tests/test_parsers.py::test_csv_parser_validates_required_fields

# With coverage
pytest --cov=orchestrator --cov-report=html
```

---

## Integration Tests

Full pipeline integration test against int environment:

```bash
make integration-test
```

This runs a comprehensive test:

1. Upload test files for all sources
2. Run full pipeline cycle
3. Verify validation logs
4. Verify dbt run logs
5. Verify data in all layers (staging → curation → consumer)
6. Verify control tables populated
7. Verify extract generated (if 06:00 run)
8. Clean up

### CI Integration Test

In `.github/workflows/ci.yml`:

```yaml
integration-test:
  runs-on: ubuntu-latest
  environment: int
  steps:
    - uses: actions/checkout@v4
    
    - name: Authenticate to GCP
      uses: google-github-actions/auth@v2
      with:
        credentials_json: ${{ secrets.GCP_SA_KEY }}
    
    - name: Run integration tests
      run: make integration-test
      env:
        PROJECT_ID: markets-int-12345
```

---

## Testing Locally

### Local dbt Testing

```bash
cd dbt_project

# Use dev target with your credentials
export DBT_TARGET=dev

# Run against int BigQuery
dbt run --select stg_murex_trades
dbt test --select stg_murex_trades
```

### Local Orchestrator Testing

```bash
cd orchestrator

# Unit tests (no GCP needed)
pytest tests/

# Integration tests (needs GCP credentials)
export PROJECT_ID=markets-int-12345
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
pytest tests/integration/
```

### Mock GCS for Unit Tests

```python
# tests/conftest.py
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_storage_client(monkeypatch):
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    
    monkeypatch.setattr(
        "google.cloud.storage.Client",
        lambda: mock_client
    )
    
    return mock_client, mock_bucket
```

---

## Test Data Management

### Fixtures

Store test fixtures in `tests/fixtures/`:

```
tests/fixtures/
├── murex/
│   ├── valid_trade.xml
│   ├── missing_required_field.xml
│   └── control_file.xml
├── venue_a/
│   ├── valid_trades.csv
│   └── bad_types.csv
└── reference/
    └── traders.csv
```

### Generating Test Data

```python
# scripts/generate_test_data.py

import csv
import random
from datetime import datetime, timedelta

def generate_trades(n: int, output_path: str):
    traders = ["TRD001", "TRD002", "TRD003"]
    instruments = ["AAPL", "MSFT", "GOOGL", "AMZN"]
    
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "trade_id", "trade_time", "trader_id", 
            "instrument", "side", "quantity", "price"
        ])
        writer.writeheader()
        
        for i in range(n):
            writer.writerow({
                "trade_id": f"TEST-{i:06d}",
                "trade_time": (datetime.now() - timedelta(hours=random.randint(0, 24))).isoformat(),
                "trader_id": random.choice(traders),
                "instrument": random.choice(instruments),
                "side": random.choice(["BUY", "SELL"]),
                "quantity": random.randint(100, 10000),
                "price": round(random.uniform(10, 500), 2),
            })

if __name__ == "__main__":
    generate_test_data(1000, "tests/fixtures/large_trade_file.csv")
```

---

## Continuous Integration

### CI Pipeline

```yaml
# .github/workflows/ci.yml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff
      - run: ruff check orchestrator/

  python-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -e "orchestrator/[dev]"
      - run: pytest orchestrator/tests/

  dbt-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install dbt-bigquery
      - run: cd dbt_project && dbt deps
      - run: cd dbt_project && dbt compile --target ci
      # Full test requires BigQuery access
      
  validate-specs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pyyaml
      - run: python scripts/validate_all_specs.py
      
  namespace-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Verify namespace unchanged
        run: |
          CURRENT=$(grep 'markets_namespace' dbt_project/dbt_project.yml | cut -d"'" -f2)
          EXPECTED="a1b2c3d4-e5f6-7890-abcd-ef1234567890"
          if [ "$CURRENT" != "$EXPECTED" ]; then
            echo "::error::Namespace UUID has changed! This will break trade_id generation."
            exit 1
          fi
```

---

## Troubleshooting Tests

| Problem | Solution |
|---------|----------|
| dbt test hangs | Check BigQuery quota; may need to reduce parallelism |
| Fixture not found | Check path is relative to test file |
| Mock not applied | Ensure monkeypatch targets import location, not definition |
| Integration test flaky | Add retries; check for async timing issues |
| Test data not cleaned up | Ensure cleanup runs even on failure (use fixtures/finally) |

---

## Summary

Testing strategy:

1. **Unit tests** — Fast, no infrastructure, catch logic errors
2. **dbt tests** — Schema validation, catch data quality issues
3. **Smoke tests** — One source end-to-end, catch integration issues
4. **Integration tests** — Full pipeline, catch system issues

Run unit tests on every commit. Run integration tests before deploy.
