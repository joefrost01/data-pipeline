"""
Configuration management for the Orchestrator.

This module handles:
- Loading environment variables into a typed Config dataclass
- Loading and parsing table mapping YAML files from a directory tree
- Resolving filenames to their table configurations using fnmatch patterns
"""

import os
import fnmatch
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass
class GoFileConfig:
    """
    Configuration for go file validation.
    
    Go files signal that a data file is complete and ready for processing.
    They can be in two formats:
    - properties: key=value pairs (use row_count_key)
    - csv: comma-separated values (use row_count_column)
    """
    pattern: str                        # Glob pattern to match go file (e.g., "trades_*.go")
    format: str                         # Either "properties" or "csv"
    row_count_key: str | None = None    # For properties format: the key containing row count
    row_count_column: int | None = None # For csv format: zero-indexed column with row count


@dataclass
class TrailerConfig:
    """
    Configuration for trailer record validation.
    
    Some files include a final row with metadata including expected row count.
    The trailer row is removed before loading.
    """
    row_count_column: int  # Zero-indexed column containing the expected row count


@dataclass
class TableConfig:
    """
    Complete configuration for a single table/file pattern.
    
    At minimum, specifies the target table name. Optionally includes
    validation configuration via go_file or trailer.
    """
    table: str                          # Target table name (e.g., "raw_trades")
    go_file: GoFileConfig | None = None # Optional go file validation config
    trailer: TrailerConfig | None = None # Optional trailer record validation config


@dataclass
class Config:
    """
    Application configuration loaded from environment variables.
    
    Supports two backends:
    - duckdb: Local development with filesystem storage
    - bigquery: Production with GCS storage
    """
    # Required paths - work for both local filesystem and GCS (gs://bucket/prefix)
    landing_path: str       # Where incoming files arrive
    archive_path: str       # Where successfully processed files are moved
    failed_path: str        # Where failed files are moved for investigation
    table_config_path: str  # Root directory containing table mapping YAML files
    
    # Processing configuration
    workers: int            # Number of parallel loading threads (default: 1)
    backend: str            # "duckdb" or "bigquery"
    
    # BigQuery-specific configuration
    gcp_project: str | None = None      # GCP project ID
    bq_dataset: str | None = None       # BigQuery dataset name
    staging_bucket: str | None = None   # GCS bucket for Parquet staging files
    
    # DuckDB-specific configuration
    duckdb_path: str | None = None      # Path to DuckDB database file
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Load configuration from environment variables.
        
        Required:
            LANDING_PATH: Where files arrive
            ARCHIVE_PATH: Where successful files go
            FAILED_PATH: Where failed files go
            TABLE_CONFIG_PATH: Directory with table mapping YAML files
        
        Optional:
            LOADER_WORKERS: Thread count (default: 1)
            LOADER_BACKEND: "duckdb" or "bigquery" (default: duckdb)
            DUCKDB_PATH: Path to database file (default: dev.duckdb)
            GCP_PROJECT: GCP project ID (required for bigquery)
            BQ_DATASET: BigQuery dataset (required for bigquery)
            STAGING_BUCKET: GCS bucket for staging (required for bigquery)
        """
        return cls(
            landing_path=os.environ["LANDING_PATH"],
            archive_path=os.environ["ARCHIVE_PATH"],
            failed_path=os.environ["FAILED_PATH"],
            table_config_path=os.environ["TABLE_CONFIG_PATH"],
            workers=int(os.environ.get("LOADER_WORKERS", "1")),
            backend=os.environ.get("LOADER_BACKEND", "duckdb"),
            gcp_project=os.environ.get("GCP_PROJECT"),
            bq_dataset=os.environ.get("BQ_DATASET"),
            staging_bucket=os.environ.get("STAGING_BUCKET"),
            duckdb_path=os.environ.get("DUCKDB_PATH", "dev.duckdb"),
        )


def load_table_mapping(root_path: str) -> dict[str, TableConfig]:
    """
    Load all table mappings from YAML files in a directory tree.
    
    Walks the entire directory tree looking for *.yaml files and merges
    all mappings into a single dictionary. This allows organizing table
    configs into logical subdirectories (e.g., trading/, reference/).
    
    Args:
        root_path: Root directory to search for YAML files
    
    Returns:
        Dictionary mapping filename patterns to TableConfig objects
    
    Example YAML formats:
        
        # Simple format - just table name
        instruments_*.csv: raw_instruments
        
        # With go file validation (properties format)
        trades_*.csv:
          table: raw_trades
          go_file:
            pattern: trades_*.go
            format: properties
            row_count_key: record_count
        
        # With go file validation (CSV format)
        positions_*.csv:
          table: raw_positions
          go_file:
            pattern: positions_*.ctl
            format: csv
            row_count_column: 2
        
        # With trailer record validation
        eod_prices_*.csv:
          table: raw_eod_prices
          trailer:
            row_count_column: 1
    """
    result = {}
    root = Path(root_path)
    
    # Walk the directory tree looking for YAML files
    for yaml_path in root.rglob("*.yaml"):
        raw = yaml.safe_load(yaml_path.read_text())
        
        # Skip empty files
        if not raw:
            continue
        
        # Parse each pattern -> config mapping in the file
        for pattern, cfg in raw.items():
            # Simple format: pattern maps directly to table name
            if isinstance(cfg, str):
                result[pattern] = TableConfig(table=cfg)
            else:
                # Complex format: pattern maps to config dict
                go_file = None
                trailer = None
                
                # Parse go file configuration if present
                if "go_file" in cfg:
                    gf = cfg["go_file"]
                    go_file = GoFileConfig(
                        pattern=gf["pattern"],
                        format=gf["format"],
                        row_count_key=gf.get("row_count_key"),
                        row_count_column=gf.get("row_count_column"),
                    )
                
                # Parse trailer configuration if present
                if "trailer" in cfg:
                    trailer = TrailerConfig(
                        row_count_column=cfg["trailer"]["row_count_column"]
                    )
                
                result[pattern] = TableConfig(
                    table=cfg["table"],
                    go_file=go_file,
                    trailer=trailer,
                )
    
    return result


def resolve_table(filename: str, mapping: dict[str, TableConfig]) -> TableConfig:
    """
    Find the TableConfig for a given filename using fnmatch patterns.
    
    Args:
        filename: The filename to match (e.g., "trades_20240115.csv")
        mapping: Dictionary of patterns to TableConfig objects
    
    Returns:
        The matching TableConfig
    
    Raises:
        ValueError: If no pattern matches the filename
    """
    for pattern, config in mapping.items():
        if fnmatch.fnmatch(filename, pattern):
            return config
    raise ValueError(f"No table mapping found for {filename}")
