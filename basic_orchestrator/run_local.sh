#!/bin/bash
# Local development setup script
# Creates required directories and runs the orchestrator

set -e

echo "Setting up local development environment..."

# Create data directories
mkdir -p data/{landing,archive,failed}

# Export environment variables
export LANDING_PATH=./data/landing
export ARCHIVE_PATH=./data/archive
export FAILED_PATH=./data/failed
export TABLE_CONFIG_PATH=./config/tables
export DUCKDB_PATH=./dev.duckdb
export LOADER_BACKEND=duckdb
export LOADER_WORKERS=1
export DBT_PROJECT_PATH=./dbt

# Note: dbt uses profiles.yml which defaults to ../dev.duckdb (relative to dbt dir)
# This is the same file as ./dev.duckdb from the orchestrator's perspective

echo "Environment:"
echo "  LANDING_PATH:     $LANDING_PATH"
echo "  ARCHIVE_PATH:     $ARCHIVE_PATH"
echo "  FAILED_PATH:      $FAILED_PATH"
echo "  TABLE_CONFIG_PATH: $TABLE_CONFIG_PATH"
echo "  DUCKDB_PATH:      $DUCKDB_PATH"
echo "  LOADER_BACKEND:   $LOADER_BACKEND"
echo "  LOADER_WORKERS:   $LOADER_WORKERS"
echo "  DBT_PROJECT_PATH: $DBT_PROJECT_PATH"
echo ""
echo "Starting orchestrator..."
echo "Drop CSV files into data/landing/ to process them"
echo ""

python -m orchestrator.main
