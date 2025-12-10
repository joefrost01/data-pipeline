"""
Main orchestration module for the Orchestrator.

This module ties everything together:
1. Polls the landing directory for new files
2. Loads files in parallel using a thread pool
3. Runs dbt after each batch
4. Handles schema drift by capturing new columns in _extra JSON field
5. Tracks all loads with metadata for full traceability

The main loop is deliberately simple - poll, load, transform, sleep, repeat.
No event infrastructure, no complex state management, just a loop.
"""

import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import polars as pl

from orchestrator.config import Config, TableConfig, load_table_mapping, resolve_table
from orchestrator.loader import DataLoader, DuckDBLoader, BigQueryLoader, LoadResult
from orchestrator.storage import Storage, LocalStorage, GCSStorage
from orchestrator.validation import (
    ValidationResult,
    find_go_file,
    validate_with_go_file,
    process_with_trailer,
)

# Configure logging with timestamp
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def create_components(config: Config) -> tuple[Storage, DataLoader]:
    """
    Create the appropriate storage and loader implementations based on config.
    
    Args:
        config: Application configuration
    
    Returns:
        Tuple of (storage, loader) implementations
    
    Raises:
        ValueError: If required config is missing for the selected backend
    """
    if config.backend == "bigquery":
        # Validate required BigQuery config
        if not all([config.gcp_project, config.bq_dataset, config.staging_bucket]):
            raise ValueError(
                "BigQuery backend requires GCP_PROJECT, BQ_DATASET, and STAGING_BUCKET"
            )
        
        return (
            GCSStorage(),
            BigQueryLoader(
                config.gcp_project,
                config.bq_dataset,
                config.staging_bucket,
            ),
        )
    else:
        # Default to DuckDB
        return (
            LocalStorage(),
            DuckDBLoader(config.duckdb_path),
        )


def prepare_dataframe(
    df: pl.DataFrame,
    existing_columns: list[str] | None
) -> pl.DataFrame:
    """
    Prepare a DataFrame for loading, handling schema drift.
    
    Schema drift handling:
    - If table doesn't exist (existing_columns is None): All columns become the schema
    - If table exists: Known columns load normally, new columns go to _extra JSON field
    - Missing expected columns are filled with NULL
    
    This allows new attributes to be used immediately via JSON extraction,
    without requiring schema changes or code deployments.
    
    Args:
        df: Raw DataFrame from CSV
        existing_columns: List of columns in existing table, or None if new table
    
    Returns:
        DataFrame ready for loading with _extra column
    """
    if existing_columns is None:
        # First load - all columns become the baseline schema
        # Add empty _extra column for consistency
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("_extra"))
    
    # Determine which columns are known vs new
    actual_columns = set(df.columns)
    expected_columns = set(existing_columns)
    
    known = [col for col in df.columns if col in expected_columns]
    extra = [col for col in df.columns if col not in expected_columns]
    
    # Handle extra columns by serialising to JSON
    if extra:
        # Build JSON object from extra columns for each row
        extra_data = df.select(extra).to_dicts()
        extra_json = [json.dumps(row) for row in extra_data]
        
        # Select known columns and add _extra
        df = df.select(known).with_columns(
            pl.Series("_extra", extra_json)
        )
        
        log.info(f"  New columns captured in _extra: {extra}")
    else:
        # No extra columns - add null _extra
        df = df.select(known).with_columns(
            pl.lit(None).cast(pl.Utf8).alias("_extra")
        )
    
    # Add NULL for any expected columns missing from this file
    for col in existing_columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(col))
            log.info(f"  Missing column filled with NULL: {col}")
    
    return df


def load_file(
    path: str,
    storage: Storage,
    loader: DataLoader,
    config: Config,
    mapping: dict[str, TableConfig],
    available_files: list[str],
) -> bool:
    """
    Load a single file into the data warehouse.
    
    Process:
    1. Resolve filename to table configuration
    2. Check for go file if required (skip if not yet available)
    3. Read CSV with all columns as strings (no type inference)
    4. Handle trailer record validation if configured
    5. Validate against go file if configured
    6. Handle schema drift (new columns -> _extra)
    7. Add _load_id to every row
    8. Load into target table
    9. Record metadata
    10. Archive the file (and go file if applicable)
    
    Args:
        path: Full path to the data file
        storage: Storage implementation
        loader: Data loader implementation
        config: Application configuration
        mapping: Table mapping configuration
        available_files: All files in landing directory (for go file matching)
    
    Returns:
        True if file was processed (success or failure), False if skipped
    
    Raises:
        Exception: Re-raises any exception after moving file to failed directory
    """
    filename = Path(path).name
    
    # Resolve table configuration
    try:
        table_config = resolve_table(filename, mapping)
    except ValueError as e:
        log.warning(f"Skipping {filename}: {e}")
        return False
    
    # Check for go file if required
    go_path = None
    if table_config.go_file:
        go_path = find_go_file(filename, table_config.go_file, available_files)
        if not go_path:
            log.debug(f"Skipping {filename} - go file not yet available")
            return False
    
    # Generate load tracking info
    load_id = str(uuid4())
    started_at = datetime.utcnow()
    
    log.info(f"Loading {filename} -> {table_config.table} [{load_id[:8]}...]")
    
    try:
        # Read CSV with all columns as strings (no type inference headaches)
        data = storage.read_file(path)
        df = pl.read_csv(data, infer_schema_length=0)
        
        log.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")
        
        # Handle trailer record validation if configured
        if table_config.trailer:
            df, validation = process_with_trailer(
                df,
                table_config.trailer.row_count_column
            )
            
            if not validation.valid:
                log.error(f"  Trailer validation failed: {validation.error}")
                storage.move(path, storage.join(config.failed_path, filename))
                return True
            
            log.info(f"  Trailer validation passed: {validation.actual_rows} rows")
        
        # Handle go file validation if configured
        if table_config.go_file and go_path:
            validation = validate_with_go_file(
                go_path,
                storage,
                table_config.go_file,
                len(df),
            )
            
            if not validation.valid:
                log.error(f"  Go file validation failed: {validation.error}")
                storage.move(path, storage.join(config.failed_path, filename))
                return True
            
            log.info(f"  Go file validation passed: {validation.actual_rows} rows")
        
        # Handle schema drift
        existing_columns = loader.get_columns(table_config.table)
        df = prepare_dataframe(df, existing_columns)
        
        # Add load tracking column
        df = df.with_columns(pl.lit(load_id).alias("_load_id"))
        
        # Load into target table
        row_count = len(df)
        loader.load(df, table_config.table)
        
        completed_at = datetime.utcnow()
        
        # Record metadata
        loader.record_metadata(LoadResult(
            load_id=load_id,
            filename=filename,
            table=table_config.table,
            row_count=row_count,
            started_at=started_at,
            completed_at=completed_at,
        ))
        
        # Archive successfully processed files
        storage.move(path, storage.join(config.archive_path, filename))
        
        # Archive go file too if present
        if go_path:
            go_filename = Path(go_path).name
            storage.move(go_path, storage.join(config.archive_path, go_filename))
        
        duration = (completed_at - started_at).total_seconds()
        log.info(f"  Completed in {duration:.1f}s")
        
        return True
    
    except Exception as e:
        log.error(f"  Failed: {e}")
        # Move to failed directory for investigation
        storage.move(path, storage.join(config.failed_path, filename))
        raise


def load_all(
    files: list[str],
    storage: Storage,
    loader: DataLoader,
    config: Config,
    mapping: dict[str, TableConfig],
    available_files: list[str],
) -> list[str]:
    """
    Load multiple files in parallel using a thread pool.
    
    Uses ThreadPoolExecutor for parallel I/O. The thread count is
    configured via LOADER_WORKERS (default: 1 for sequential processing).
    
    Even with workers=1, using the executor provides consistent code paths
    and makes scaling up trivial when needed.
    
    Args:
        files: List of data file paths to load
        storage: Storage implementation
        loader: Data loader implementation
        config: Application configuration
        mapping: Table mapping configuration
        available_files: All files in landing (for go file matching)
    
    Returns:
        List of filenames that failed to load
    """
    errors = []
    
    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        # Submit all files for processing
        future_to_file = {
            executor.submit(
                load_file,
                f,
                storage,
                loader,
                config,
                mapping,
                available_files,
            ): f
            for f in files
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            path = future_to_file[future]
            filename = Path(path).name
            
            try:
                future.result()
            except Exception as e:
                log.error(f"Failed to load {filename}: {e}")
                errors.append(filename)
    
    return errors


def run_dbt() -> bool:
    """
    Execute dbt run to transform loaded data.
    
    Runs dbt as a subprocess and captures output. Logs stderr on failure.
    
    Returns:
        True if dbt completed successfully, False otherwise
    """
    log.info("Running dbt...")
    
    result = subprocess.run(
        ["dbt", "run"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        log.error(f"dbt failed with return code {result.returncode}")
        log.error(f"stderr: {result.stderr}")
        return False
    
    log.info("dbt completed successfully")
    return True


def get_data_files(files: list[str], mapping: dict[str, TableConfig]) -> list[str]:
    """
    Filter file list to only include data files (not go/control files).
    
    Go files and control files are processed alongside their data files,
    not independently. This function identifies which files are actual
    data files that should be processed.
    
    Args:
        files: All files in the landing directory
        mapping: Table mapping configuration
    
    Returns:
        List of data file paths
    """
    # Collect all go file patterns
    go_patterns = set()
    for table_config in mapping.values():
        if table_config.go_file:
            go_patterns.add(table_config.go_file.pattern)
    
    # Filter to data files only
    data_files = []
    for filepath in files:
        filename = Path(filepath).name
        
        # Check if this file matches any go file pattern
        is_go_file = any(
            __import__("fnmatch").fnmatch(filename, pattern)
            for pattern in go_patterns
        )
        
        if not is_go_file:
            data_files.append(filepath)
    
    return data_files


def main() -> None:
    """
    Main orchestration loop.
    
    Runs continuously:
    1. Reload table mapping (picks up ConfigMap changes)
    2. List files in landing directory
    3. Load any data files found
    4. Run dbt if files were loaded
    5. Sleep and repeat
    
    The loop continues indefinitely until the process is terminated.
    """
    # Load configuration from environment
    config = Config.from_env()
    
    # Create storage and loader implementations
    storage, loader = create_components(config)
    
    log.info(f"Orchestrator starting")
    log.info(f"  Backend: {config.backend}")
    log.info(f"  Workers: {config.workers}")
    log.info(f"  Landing: {config.landing_path}")
    log.info(f"  Archive: {config.archive_path}")
    log.info(f"  Failed:  {config.failed_path}")
    log.info(f"  Config:  {config.table_config_path}")
    
    while True:
        try:
            # Reload table mapping each iteration
            # This allows ConfigMap updates to take effect without restart
            mapping = load_table_mapping(config.table_config_path)
            
            # List all files in landing directory
            all_files = storage.list_files(config.landing_path)
            
            # Filter to data files only (exclude go/control files)
            data_files = get_data_files(all_files, mapping)
            
            if data_files:
                log.info(f"Found {len(data_files)} data files to process")
                
                # Load all files (parallel if workers > 1)
                errors = load_all(
                    data_files,
                    storage,
                    loader,
                    config,
                    mapping,
                    all_files,
                )
                
                # Run dbt after loading
                run_dbt()
                
                if errors:
                    log.warning(f"Batch completed with {len(errors)} failures: {errors}")
                else:
                    log.info("Batch completed successfully")
        
        except Exception as e:
            log.error(f"Error in main loop: {e}")
        
        # Sleep before next poll
        time.sleep(10)


if __name__ == "__main__":
    main()
