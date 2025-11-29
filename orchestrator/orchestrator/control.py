"""Write to BigQuery control tables."""

from datetime import datetime, timezone
from typing import Any

import structlog
from google.cloud import bigquery

from orchestrator.orchestrator.config import Config

log = structlog.get_logger()


class ControlTableWriter:
    """Writes audit records to control tables."""
    
    def __init__(self, config: Config) -> None:
        self.config = config
        self.client = bigquery.Client(location=config.bq_location)
        self.dataset = config.control_dataset
    
    def log_validation(
        self,
        run_id: str,
        source_name: str,
        file_path: str,
        row_count: int,
        passed: bool,
        failure_reason: str | None,
        quarantined_rows: int,
        output_path: str | None,
        file_size_bytes: int | None = None,
        expected_row_count: int | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        """Log a validation run to control.validation_runs."""
        row = {
            "run_id": run_id,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "source_name": source_name,
            "file_path": file_path,
            "file_size_bytes": file_size_bytes,
            "row_count": row_count,
            "expected_row_count": expected_row_count,
            "passed": passed,
            "failure_reason": failure_reason,
            "quarantined_rows": quarantined_rows,
            "output_path": output_path,
            "duration_seconds": duration_seconds,
        }
        
        self._insert_row(f"{self.dataset}.validation_runs", row)
    
    def log_dbt_run(
        self,
        run_id: str,
        invocation_id: str,
        model_name: str,
        status: str,
        rows_affected: int,
        execution_time_seconds: float,
        error_message: str | None = None,
        bytes_processed: int | None = None,
    ) -> None:
        """Log a dbt model run to control.dbt_runs."""
        row = {
            "run_id": run_id,
            "run_timestamp": datetime.now(timezone.utc).isoformat(),
            "invocation_id": invocation_id,
            "model_name": model_name,
            "status": status,
            "rows_affected": rows_affected,
            "execution_time_seconds": execution_time_seconds,
            "bytes_processed": bytes_processed,
            "error_message": error_message,
        }
        
        self._insert_row(f"{self.dataset}.dbt_runs", row)
    
    def log_source_completeness(
        self,
        source_name: str,
        business_date: str,
        status: str,
        file_count: int,
        total_rows: int | None = None,
        first_file_at: str | None = None,
        last_file_at: str | None = None,
        expected_by: str | None = None,
        consecutive_missing_days: int | None = None,
    ) -> None:
        """Log source completeness to control.source_completeness."""
        row = {
            "business_date": business_date,
            "source_name": source_name,
            "expected_by": expected_by,
            "first_file_at": first_file_at,
            "last_file_at": last_file_at,
            "file_count": file_count,
            "total_rows": total_rows,
            "status": status,
            "consecutive_missing_days": consecutive_missing_days,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        
        self._insert_row(f"{self.dataset}.source_completeness", row)
    
    def _insert_row(self, table_id: str, row: dict[str, Any]) -> None:
        """Insert a single row to BigQuery."""
        # Filter out None values for cleaner inserts
        row = {k: v for k, v in row.items() if v is not None}
        
        try:
            errors = self.client.insert_rows_json(table_id, [row])
            
            if errors:
                log.error(
                    "bigquery_insert_failed",
                    table=table_id,
                    errors=errors,
                )
            else:
                log.debug("control_row_inserted", table=table_id)
        except Exception as e:
            # Log but don't fail the pipeline for control table issues
            log.error(
                "bigquery_insert_exception",
                table=table_id,
                error=str(e),
            )
