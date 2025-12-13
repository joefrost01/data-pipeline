"""
Main orchestration module for the Orchestrator.

This module ties everything together:
1. Polls the landing directory for new files
2. Loads files in parallel using a thread pool
3. Runs dbt after each batch
4. Handles schema drift by capturing new columns in _extra JSON field
5. Tracks all loads with metadata for full traceability

Key resilience principles:
1. The main loop never dies
2. Individual file failures don't block other files
3. Infrastructure failures are logged and retried next cycle
4. All errors emit structured logs for Dynatrace

The main loop is deliberately simple - poll, load, transform, sleep, repeat.
No event infrastructure, no complex state management, just a loop.
"""

import json
import logging
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
from typing import Any

import polars as pl

from orchestrator.config import Config, TableConfig, load_table_mapping, resolve_table
from orchestrator.loader import DataLoader, DuckDBLoader, BigQueryLoader, LoadResult
from orchestrator.storage import Storage, LocalStorage, GCSStorage
from orchestrator.validation import (
    find_go_file,
    validate_with_go_file,
    process_with_trailer,
)


class StructuredLogger:
    """
    Logger that emits JSON for Dynatrace ingestion.

    Dynatrace can parse JSON logs and extract fields for alerting,
    dashboards, and correlation with traces.
    """

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)

    def _emit(self, level: str, message: str, **context: Any) -> None:
        """Emit a structured log entry."""
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "message": message,
            "service": "orchestrator",
            **context,
        }

        log_method = getattr(self.logger, level.lower())
        log_method(json.dumps(entry))

    def info(self, message: str, **context: Any) -> None:
        self._emit("INFO", message, **context)

    def warning(self, message: str, **context: Any) -> None:
        self._emit("WARNING", message, **context)

    def error(self, message: str, **context: Any) -> None:
        self._emit("ERROR", message, **context)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",  # Just the message - it's already JSON
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = StructuredLogger(__name__)


def create_components(config: Config) -> tuple[Storage, DataLoader] | tuple[None, None]:
    """
    Create storage and loader, returning (None, None) on failure.

    This allows the main loop to continue and retry next cycle
    rather than crashing on transient infrastructure issues.
    """
    try:
        if config.backend == "bigquery":
            if not all([config.gcp_project, config.bq_dataset, config.staging_bucket]):
                log.error(
                    "Missing BigQuery configuration",
                    backend=config.backend,
                    has_project=bool(config.gcp_project),
                    has_dataset=bool(config.bq_dataset),
                    has_bucket=bool(config.staging_bucket),
                )
                return None, None

            return (
                GCSStorage(),
                BigQueryLoader(
                    config.gcp_project,
                    config.bq_dataset,
                    config.staging_bucket,
                ),
            )
        else:
            return (
                LocalStorage(),
                DuckDBLoader(config.duckdb_path),
            )
    except Exception as e:
        log.error(
            "Failed to create components",
            error=str(e),
            error_type=type(e).__name__,
            backend=config.backend,
        )
        return None, None


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
) -> dict[str, Any]:
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
        Dict with keys: success, filename, table, rows, duration, error
    """
    filename = Path(path).name
    started_at = datetime.now(timezone.utc)
    load_id = str(uuid4())

    result = {
        "success": False,
        "filename": filename,
        "load_id": load_id,
        "table": None,
        "rows": 0,
        "duration_seconds": 0,
        "error": None,
        "error_type": None,
        "skipped": False,
    }

    try:
        # Resolve table configuration
        try:
            table_config = resolve_table(filename, mapping)
            result["table"] = table_config.table
        except ValueError as e:
            result["skipped"] = True
            result["error"] = str(e)
            return result

        # Check for go file if required
        go_path = None
        if table_config.go_file:
            go_path = find_go_file(filename, table_config.go_file, available_files)
            if not go_path:
                result["skipped"] = True
                result["error"] = "Go file not yet available"
                return result

        # Read CSV
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
                result["error"] = f"Trailer validation: {validation.error}"
                storage.move(path, storage.join(config.failed_path, filename))
                return result

        # Go file validation
        if table_config.go_file and go_path:
            validation = validate_with_go_file(
                go_path, storage, table_config.go_file, len(df)
            )
            if not validation.valid:
                result["error"] = f"Go file validation: {validation.error}"
                storage.move(path, storage.join(config.failed_path, filename))
                return result

        # Schema drift handling
        existing_columns = loader.get_columns(table_config.table)
        df = prepare_dataframe(df, existing_columns)
        df = df.with_columns(pl.lit(load_id).alias("_load_id"))

        # Load
        result["rows"] = len(df)
        loader.load(df, table_config.table)

        completed_at = datetime.now(timezone.utc)

        # Record metadata
        loader.record_metadata(LoadResult(
            load_id=load_id,
            filename=filename,
            table=table_config.table,
            row_count=len(df),
            started_at=started_at,
            completed_at=completed_at,
        ))

        # Archive
        storage.move(path, storage.join(config.archive_path, filename))

        # Archive go file too if present
        if go_path:
            go_filename = Path(go_path).name
            storage.move(go_path, storage.join(config.archive_path, go_filename))

        result["success"] = True
        result["duration_seconds"] = (completed_at - started_at).total_seconds()

    except Exception as e:
        result["error"] = str(e)
        result["error_type"] = type(e).__name__

        # Try to move to failed - but don't fail if this fails
        try:
            storage.move(path, storage.join(config.failed_path, filename))
        except Exception as move_error:
            log.error(
                "Failed to move file to failed directory",
                filename=filename,
                error=str(move_error),
            )

    return result


def load_all(
    files: list[str],
    storage: Storage,
    loader: DataLoader,
    config: Config,
    mapping: dict[str, TableConfig],
    available_files: list[str],
) -> dict[str, Any]:
    """
    Load all files, returning aggregate results.

    Never raises - all errors are captured in the results dict.
    """
    results = {
        "total": len(files),
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "total_rows": 0,
        "failures": [],
    }

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        # Submit all files for processing
        future_to_file = {
            executor.submit(
                load_file, f, storage, loader, config, mapping, available_files
            ): f
            for f in files
        }

        for future in as_completed(future_to_file):
            try:
                result = future.result()

                if result["skipped"]:
                    results["skipped"] += 1
                elif result["success"]:
                    results["succeeded"] += 1
                    results["total_rows"] += result["rows"]
                    log.info(
                        "File loaded successfully",
                        filename=result["filename"],
                        table=result["table"],
                        rows=result["rows"],
                        duration_seconds=result["duration_seconds"],
                        load_id=result["load_id"],
                    )
                else:
                    results["failed"] += 1
                    results["failures"].append(result)
                    log.error(
                        "File load failed",
                        filename=result["filename"],
                        table=result["table"],
                        error=result["error"],
                        error_type=result["error_type"],
                        load_id=result["load_id"],
                    )

            except Exception as e:
                # This shouldn't happen, but catch it anyway
                results["failed"] += 1
                log.error(
                    "Unexpected error in thread",
                    error=str(e),
                    error_type=type(e).__name__,
                )

    return results


def run_dbt(dbt_project_dir: str | None = None) -> dict[str, Any]:
    """
    Run dbt with resilience.

    Returns result dict rather than bool, captures all output.

    Args:
        dbt_project_dir: Path to dbt project directory. If None, runs from current dir.
    """
    result = {
        "success": False,
        "return_code": None,
        "duration_seconds": 0,
        "error": None,
    }

    started_at = datetime.now(timezone.utc)

    try:
        # Create environment for dbt subprocess
        # Remove DUCKDB_PATH so dbt uses its profiles.yml default (../dev.duckdb)
        # This ensures both orchestrator and dbt use the same database file
        dbt_env = os.environ.copy()
        dbt_env.pop("DUCKDB_PATH", None)

        proc = subprocess.run(
            ["dbt", "run", "--no-fail-fast", "--profiles-dir", "."],  # Use profiles.yml from project dir
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
            cwd=dbt_project_dir,  # Run from dbt project directory
            env=dbt_env,
        )

        result["return_code"] = proc.returncode
        result["success"] = proc.returncode == 0

        if not result["success"]:
            # dbt writes errors to stdout, not stderr
            error_output = proc.stdout or proc.stderr or "dbt returned non-zero exit code"
            result["error"] = error_output
            log.error(
                "dbt run completed with failures",
                return_code=proc.returncode,
                stdout=proc.stdout[:2000] if proc.stdout else None,
                stderr=proc.stderr[:1000] if proc.stderr else None,
            )
        else:
            log.info("dbt run completed successfully")

    except subprocess.TimeoutExpired:
        result["error"] = "dbt run timed out after 1 hour"
        log.error("dbt run timed out", timeout_seconds=3600)

    except FileNotFoundError:
        result["error"] = "dbt command not found"
        log.error("dbt command not found - is dbt installed?")

    except Exception as e:
        result["error"] = str(e)
        log.error(
            "dbt run failed unexpectedly",
            error=str(e),
            error_type=type(e).__name__,
        )

    result["duration_seconds"] = (datetime.now(timezone.utc) - started_at).total_seconds()
    return result


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

    import fnmatch
    data_files = []
    for filepath in files:
        filename = Path(filepath).name
        is_go_file = any(fnmatch.fnmatch(filename, p) for p in go_patterns)
        if not is_go_file:
            data_files.append(filepath)

    return data_files


def main() -> None:
    """
    Main loop - designed to never die.

    Every operation is wrapped in try/catch. Infrastructure failures
    cause a retry next cycle. The loop only exits on SIGTERM/SIGINT.
    """
    log.info("Orchestrator starting")

    # Load config - this can fail, but we retry
    config = None
    while config is None:
        try:
            config = Config.from_env()
        except Exception as e:
            log.error(
                "Failed to load configuration - retrying in 30s",
                error=str(e),
                error_type=type(e).__name__,
            )
            time.sleep(30)

    log.info(
        "Configuration loaded",
        backend=config.backend,
        workers=config.workers,
        landing_path=config.landing_path,
    )

    # Track consecutive infrastructure failures for backoff
    consecutive_infra_failures = 0
    max_backoff_seconds = 300  # 5 minutes max

    while True:
        cycle_start = datetime.now(timezone.utc)

        try:
            # Create components - may fail transiently
            storage, loader = create_components(config)

            if storage is None or loader is None:
                consecutive_infra_failures += 1
                backoff = min(30 * consecutive_infra_failures, max_backoff_seconds)
                log.warning(
                    "Infrastructure not available - backing off",
                    backoff_seconds=backoff,
                    consecutive_failures=consecutive_infra_failures,
                )
                time.sleep(backoff)
                continue

            # Reset backoff on successful connection
            consecutive_infra_failures = 0

            # Load table mapping
            try:
                mapping = load_table_mapping(config.table_config_path)
            except Exception as e:
                log.error(
                    "Failed to load table mapping",
                    error=str(e),
                    config_path=config.table_config_path,
                )
                time.sleep(30)
                continue

            # List files
            try:
                all_files = storage.list_files(config.landing_path)
            except Exception as e:
                log.error(
                    "Failed to list landing directory",
                    error=str(e),
                    landing_path=config.landing_path,
                )
                time.sleep(30)
                continue

            # Filter and process
            data_files = get_data_files(all_files, mapping)

            if data_files:
                log.info(
                    "Processing batch",
                    file_count=len(data_files),
                    total_files_in_landing=len(all_files),
                )

                # Load files
                load_results = load_all(
                    data_files, storage, loader, config, mapping, all_files
                )

                # Run dbt (even if some loads failed - process what we can)
                if load_results["succeeded"] > 0:
                    # Close DuckDB connection before running dbt to ensure all writes are visible
                    if hasattr(loader, 'conn'):
                        loader.conn.close()

                    dbt_path = os.environ.get("DBT_PROJECT_PATH", "./dbt")
                    dbt_result = run_dbt(dbt_project_dir=dbt_path)
                else:
                    dbt_result = {"success": True, "skipped": True}

                # Log batch summary
                log.info(
                    "Batch complete",
                    files_succeeded=load_results["succeeded"],
                    files_failed=load_results["failed"],
                    files_skipped=load_results["skipped"],
                    total_rows=load_results["total_rows"],
                    dbt_success=dbt_result["success"],
                    cycle_duration_seconds=(datetime.now(timezone.utc) - cycle_start).total_seconds(),
                )
        
        except KeyboardInterrupt:
            log.info("Shutdown requested")
            break

        except Exception as e:
            # Catch-all for anything we missed
            log.error(
                "Unexpected error in main loop",
                error=str(e),
                error_type=type(e).__name__,
            )
        
        # Sleep before next poll
        time.sleep(10)


if __name__ == "__main__":
    main()