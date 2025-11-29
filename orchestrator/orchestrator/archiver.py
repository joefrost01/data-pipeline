"""Archive processed files from staging to archive bucket.

Only archives files that have been successfully validated in this run,
preventing the race condition where files arriving during processing get
archived without being processed.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from google.cloud import storage

from orchestrator.orchestrator.config import Config

log = structlog.get_logger()


def normalise_gcs_path(path: str) -> str:
    """Normalise a GCS path for consistent comparison.
    
    Handles:
    - URL encoding differences
    - Trailing slashes
    - Double slashes
    - gs:// prefix presence/absence
    
    Args:
        path: A GCS path (with or without gs:// prefix)
        
    Returns:
        Normalised path in format: bucket/object/path
    """
    from urllib.parse import unquote
    
    # URL decode
    path = unquote(path)
    
    # Remove gs:// prefix if present
    if path.startswith("gs://"):
        path = path[5:]
    
    # Remove leading/trailing slashes
    path = path.strip("/")
    
    # Collapse multiple slashes
    while "//" in path:
        path = path.replace("//", "/")
    
    return path


def extract_blob_name_from_path(gcs_path: str) -> str:
    """Extract the blob name (object path) from a full GCS path.
    
    Args:
        gcs_path: Full GCS path like 'gs://bucket/path/to/file.parquet'
        
    Returns:
        Blob name like 'path/to/file.parquet'
    """
    normalised = normalise_gcs_path(gcs_path)
    # Skip the bucket name (first component)
    parts = normalised.split("/", 1)
    return parts[1] if len(parts) > 1 else ""


@dataclass
class ArchiveResult:
    """Result of archive operation."""
    files_moved: int
    files_skipped: int
    archive_path: str


class Archiver:
    """Moves processed files from staging to archive bucket.
    
    Only archives files that were validated in the current run.
    The set of validated files is passed directly from the validator,
    avoiding any race conditions from re-querying the database.
    """
    
    def __init__(self, config: Config, validated_output_paths: set[str]) -> None:
        """Initialize archiver.
        
        Args:
            config: Pipeline configuration
            validated_output_paths: Set of GCS paths (gs://bucket/path) that were
                successfully validated in this run. Only these files will be archived.
        """
        self.config = config
        # Normalise all validated paths for consistent comparison
        self._validated_blob_names = {
            extract_blob_name_from_path(p) for p in validated_output_paths
        }
        self.storage_client = storage.Client()
        
        log.debug(
            "archiver_initialised",
            validated_count=len(self._validated_blob_names),
            sample_paths=list(self._validated_blob_names)[:3],
        )
    
    def run(self) -> ArchiveResult:
        """Move validated files from staging to archive."""
        staging_bucket = self.storage_client.bucket(self.config.staging_bucket)
        archive_bucket = self.storage_client.bucket(self.config.archive_bucket)
        
        # Generate archive path: archive/YYYY-MM-DD/HHMM/
        now = datetime.now(timezone.utc)
        archive_prefix = f"{now.strftime('%Y-%m-%d')}/{now.strftime('%H%M')}/"
        
        files_moved = 0
        files_skipped = 0
        
        for blob in staging_bucket.list_blobs():
            if blob.name.endswith("/"):
                continue
            
            # Check if this blob was validated in THIS run using normalised comparison
            if blob.name not in self._validated_blob_names:
                log.debug(
                    "file_skipped_not_validated_this_run",
                    file=blob.name,
                    reason="not in current run's validated files",
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
