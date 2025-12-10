# Orchestrator

A simple, polling-based data loader for batch file ingestion into DuckDB (local development) or BigQuery (production).

## Overview

Orchestrator watches a landing directory for CSV files, loads them into a data warehouse, runs dbt transformations, and repeats. It's deliberately simple - no event infrastructure, no complex orchestration frameworks, just a loop.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Orchestrator Loop                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│   │  Poll    │───▶│  Load    │───▶│  Run     │───▶│  Sleep   │──┐   │
│   │  Files   │    │  Files   │    │  dbt     │    │  10s     │  │   │
│   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │   │
│        ▲                                                        │   │
│        └────────────────────────────────────────────────────────┘   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Dual backend support**: Local filesystem + DuckDB for development, GCS + BigQuery for production
- **Schema drift handling**: Known columns load normally, new columns captured in `_extra` JSON field
- **Load tracking**: Every row stamped with `_load_id`, metadata recorded in `load_metadata` table
- **Validation**: Support for go files (properties or CSV format) and trailer records
- **Parallel loading**: Configurable thread pool (default: 1 worker)
- **File lifecycle**: Landing → Archive (success) or Failed (error)

## Architecture

### Components

| Component | Local Dev | Production |
|-----------|-----------|------------|
| Storage | Local filesystem | GCS |
| Data Warehouse | DuckDB | BigQuery |
| File Format | CSV (all strings) | CSV → Parquet |
| Config | Local YAML files | ConfigMaps |

### File Flow

```
Landing Zone              Archive                  Failed
────────────             ─────────                ──────
trades_20240115.csv  ──▶ trades_20240115.csv     
trades_20240115.go   ──▶ trades_20240115.go      
positions_20240115.csv ─────────────────────────▶ positions_20240115.csv
                                                  (validation failed)
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LANDING_PATH` | Yes | - | Where files arrive (local path or `gs://bucket/prefix`) |
| `ARCHIVE_PATH` | Yes | - | Where successful files move to |
| `FAILED_PATH` | Yes | - | Where failed files move to |
| `TABLE_CONFIG_PATH` | Yes | - | Root directory containing table mapping YAML files |
| `LOADER_BACKEND` | No | `duckdb` | `duckdb` or `bigquery` |
| `LOADER_WORKERS` | No | `1` | Number of parallel loading threads |
| `DUCKDB_PATH` | No | `dev.duckdb` | Path to DuckDB database (local only) |
| `GCP_PROJECT` | No | - | GCP project ID (BigQuery only) |
| `BQ_DATASET` | No | - | BigQuery dataset name |
| `STAGING_BUCKET` | No | - | GCS bucket for Parquet staging |

### Table Mapping

Table mappings are YAML files that can be organised into subdirectories. The orchestrator walks the entire `TABLE_CONFIG_PATH` directory tree looking for `*.yaml` files.

**Simple mapping** (no validation):

```yaml
instruments_*.csv: raw_instruments
```

**With go file validation** (properties format):

```yaml
trades_*.csv:
  table: raw_trades
  go_file:
    pattern: trades_*.go
    format: properties
    row_count_key: record_count
```

**With go file validation** (CSV format):

```yaml
positions_*.csv:
  table: raw_positions
  go_file:
    pattern: positions_*.ctl
    format: csv
    row_count_column: 2
```

**With trailer record validation**:

```yaml
eod_prices_*.csv:
  table: raw_eod_prices
  trailer:
    row_count_column: 1
```

### Example Directory Structure

```
config/
└── tables/
    ├── trading/
    │   ├── trades.yaml
    │   └── positions.yaml
    ├── reference/
    │   └── instruments.yaml
    └── market_data/
        └── eod_prices.yaml
```

## Schema Drift Handling

When a CSV contains columns not present in the existing table:

1. **Known columns** → Load into their respective columns
2. **New columns** → Serialised as JSON into `_extra` column
3. **Missing columns** → Filled with NULL

This allows new attributes to be used immediately via JSON extraction:

```sql
-- BigQuery
SELECT 
    trade_id,
    JSON_VALUE(_extra, '$.new_field') as new_field
FROM raw_trades
WHERE _extra IS NOT NULL

-- DuckDB
SELECT 
    trade_id,
    _extra::JSON->>'new_field' as new_field
FROM raw_trades
WHERE _extra IS NOT NULL
```

When a field becomes permanent, promote it:

```sql
ALTER TABLE raw_trades ADD COLUMN new_field STRING;
UPDATE raw_trades SET new_field = JSON_VALUE(_extra, '$.new_field') WHERE _extra IS NOT NULL;
```

## Load Metadata

Every load is tracked in `load_metadata`:

| Column | Type | Description |
|--------|------|-------------|
| `load_id` | STRING | UUID for this load |
| `filename` | STRING | Source filename |
| `table_name` | STRING | Target table |
| `row_count` | INTEGER | Rows loaded |
| `started_at` | TIMESTAMP | Load start time |
| `completed_at` | TIMESTAMP | Load end time |

Every row in raw tables has `_load_id` for traceability:

```sql
SELECT r.*, m.filename, m.started_at
FROM raw_trades r
JOIN load_metadata m ON r._load_id = m.load_id
WHERE m.filename LIKE 'trades_20240115%'
```

## Validation

### Go Files

Go files signal that a data file is complete and provide expected row counts.

**Properties format** (`key=value`):
```
record_count=12345
timestamp=2024-01-15T10:30:00Z
```

**CSV format**:
```
trades_20240115.csv,12345,SUCCESS
```

### Trailer Records

Some files include a trailer row with metadata. The orchestrator removes this row before loading and validates the count:

```csv
trade_id,trade_date,quantity
T001,2024-01-15,100
T002,2024-01-15,200
TRAILER,2,EOF
```

## Local Development

### Setup

```bash
# Create directories
mkdir -p data/{landing,archive,failed}

# Set environment
export LANDING_PATH=./data/landing
export ARCHIVE_PATH=./data/archive
export FAILED_PATH=./data/failed
export TABLE_CONFIG_PATH=./config/tables
export DUCKDB_PATH=./dev.duckdb
export LOADER_BACKEND=duckdb

# Run
python -m orchestrator.main
```

### Workflow

1. Drop CSV files into `data/landing/`
2. Orchestrator loads them into DuckDB
3. dbt runs against DuckDB
4. Iterate on dbt models with sub-second feedback

## Production Deployment (GKE)

### Helm Values

```yaml
env:
  LANDING_PATH: gs://myproject-data/landing
  ARCHIVE_PATH: gs://myproject-data/archive
  FAILED_PATH: gs://myproject-data/failed
  TABLE_CONFIG_PATH: /etc/orchestrator/tables
  STAGING_BUCKET: myproject-data
  GCP_PROJECT: myproject
  BQ_DATASET: raw
  LOADER_BACKEND: bigquery
  LOADER_WORKERS: "4"
```

### ConfigMap for Table Mappings

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator-table-config
data:
  trades.yaml: |
    trades_*.csv:
      table: raw_trades
      go_file:
        pattern: trades_*.go
        format: properties
        row_count_key: record_count
```

### Deployment Mount

```yaml
spec:
  containers:
    - name: orchestrator
      volumeMounts:
        - name: table-config
          mountPath: /etc/orchestrator/tables
          readOnly: true
  volumes:
    - name: table-config
      configMap:
        name: orchestrator-table-config
```

## Performance Characteristics

| Operation | 100M rows × 10 cols | Notes |
|-----------|---------------------|-------|
| Polars CSV read | ~1 min | All strings, no inference |
| DuckDB load | ~30 sec | Zero-copy via Arrow |
| BigQuery via GCS | 5-10 min | Parquet staging |

Threading provides near-linear speedup for I/O-bound operations until you hit network or quota limits.

## Design Principles

1. **Simplicity over features**: A polling loop beats event infrastructure for this use case
2. **Table as schema source**: No schema config to maintain; inspect the table, handle drift
3. **Immediate usability**: New attributes available instantly via `_extra`
4. **Observable state**: File location tells you status; `load_metadata` tells you history
5. **Local-prod parity**: Same code path, different backends via config
