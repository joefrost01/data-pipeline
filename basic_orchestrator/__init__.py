"""
Orchestrator - A simple batch data loader for DuckDB and BigQuery.

A polling-based orchestrator that watches for CSV files, loads them
into a data warehouse with schema drift handling, and runs dbt.

Usage:
    python -m orchestrator.main

Environment Variables:
    LANDING_PATH: Where files arrive
    ARCHIVE_PATH: Where successful files go
    FAILED_PATH: Where failed files go
    TABLE_CONFIG_PATH: Directory with table mapping YAML files
    LOADER_BACKEND: "duckdb" or "bigquery" (default: duckdb)
    LOADER_WORKERS: Thread count (default: 1)
"""

__version__ = "0.1.0"
