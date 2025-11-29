"""Archive processed files from staging to archive bucket.

Only archives files that have been successfully processed by dbt in this run,
preventing the race condition where files arriving during processing get
archived without being processed.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from google.cloud import bigquery, storage

from orchestrator.config import Config

log = structlog.get_logger()


@dataclass
class ArchiveResult:
    """Result of archive operation."""
    files_moved: int
    files_skipped: int
    archive_path: str


class Archiver:
    """Moves processed files from staging to archive bucket.
    
    Only archives files that:
    1. Have a corresponding validation_runs record
    2. Were validated before the current run started
    
    This prevents archiving files that arrived during the dbt run.
    """
    
    def __init__(self, config: Config, run_start_time: datetime) -> None:
        self.config = config
        self.run_start_time = run_start_time
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client(location=config.bq_location)
    
    def run(self) -> ArchiveResult:
        """Move processed files from staging to archive."""
        staging_bucket = self.storage_client.bucket(self.config.staging_bucket)
        archive_bucket = self.storage_client.bucket(self.config.archive_bucket)
        
        # Generate archive path: archive/YYYY-MM-DD/HHMM/
        now = datetime.now(timezone.utc)
        archive_prefix = f"{now.strftime('%Y-%m-%d')}/{now.strftime('%H%M')}/"
        
        # Get list of files that were validated before this run started
        processed_files = self._get_processed_files()
        
        files_moved = 0
        files_skipped = 0
        
        for blob in staging_bucket.list_blobs():
            if blob.name.endswith("/"):
                continue
            
            # Only archive if this file was processed before run started
            blob_path = f"gs://{staging_bucket.name}/{blob.name}"
            
            if blob_path not in processed_files:
                log.debug(
                    "file_skipped_not_in_processed",
                    file=blob.name,
                    reason="not in validation_runs or arrived after run start",
                )
                files_skipped += 1
                continue
            
            # Additional safety check: file modification time
            if blob.updated and blob.updated > self.run_start_time:
                log.warning(
                    "file_skipped_modified_during_run",
                    file=blob.name,
                    modified=blob.updated.isoformat(),
                    run_start=self.run_start_time.isoformat(),
                )
                files_skipped += 1
                continue
            
            # Destination path preserves source structure
            dest_name = f"{archive_prefix}{blob.name}"
            dest_blob = archive_bucket.blob(dest_name)
            
            # Copy then delete (atomic move not supported across buckets)
            dest_blob.rewrite(blob)
            blob.delete()
            
            files_moved += 1
            log.debug("file_archived", source=blob.name, destination=dest_name)
        
        log.info(
            "archive_complete",
            files_moved=files_moved,
            files_skipped=files_skipped,
            archive_path=f"gs://{archive_bucket.name}/{archive_prefix}",
        )
        
        return ArchiveResult(
            files_moved=files_moved,
            files_skipped=files_skipped,
            archive_path=f"gs://{archive_bucket.name}/{archive_prefix}",
        )
    
    def _get_processed_files(self) -> set[str]:
        """Get set of file paths that were validated before this run started.
        
        Queries validation_runs to find files that:
        - Passed validation
        - Have output_path in staging bucket
        - Were validated before the current run started
        """
        query = """
            SELECT DISTINCT output_path
            FROM control.validation_runs
            WHERE passed = TRUE
              AND output_path IS NOT NULL
              AND output_path LIKE @staging_pattern
              AND run_timestamp < @run_start_time
              AND run_timestamp >= TIMESTAMP_SUB(@run_start_time, INTERVAL 24 HOUR)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "staging_pattern",
                    "STRING",
                    f"gs://{self.config.staging_bucket}/%",
                ),
                bigquery.ScalarQueryParameter(
                    "run_start_time",
                    "TIMESTAMP",
                    self.run_start_time.isoformat(),
                ),
            ]
        )
        
        try:
            results = self.bq_client.query(query, job_config=job_config).result()
            processed = {row.output_path for row in results}
            log.debug("processed_files_found", count=len(processed))
            return processed
        except Exception as e:
            log.error("failed_to_get_processed_files", error=str(e))
            # Return empty set - safer to skip archiving than to archive unprocessed files
            return set()


class ArchiverLegacy:
    """Legacy archiver that moves all files - use Archiver instead.
    
    Kept for reference but should not be used in production due to
    race condition where files arriving during processing get archived.
    """
    
    def __init__(self, config: Config) -> None:
        self.config = config
        self.storage_client = storage.Client()
    
    def run(self) -> ArchiveResult:
        """Move all files from staging to archive."""
        staging_bucket = self.storage_client.bucket(self.config.staging_bucket)
        archive_bucket = self.storage_client.bucket(self.config.archive_bucket)
        
        now = datetime.now(timezone.utc)
        archive_prefix = f"{now.strftime('%Y-%m-%d')}/{now.strftime('%H%M')}/"
        
        files_moved = 0
        
        for blob in staging_bucket.list_blobs():
            if blob.name.endswith("/"):
                continue
            
            dest_name = f"{archive_prefix}{blob.name}"
            dest_blob = archive_bucket.blob(dest_name)
            
            dest_blob.rewrite(blob)
            blob.delete()
            
            files_moved += 1
        
        return ArchiveResult(
            files_moved=files_moved,
            files_skipped=0,
            archive_path=f"gs://{archive_bucket.name}/{archive_prefix}",
        )
