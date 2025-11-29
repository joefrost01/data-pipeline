"""Archive processed files from staging to archive bucket."""

from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from google.cloud import storage

from orchestrator.config import Config

log = structlog.get_logger()


@dataclass
class ArchiveResult:
    """Result of archive operation."""
    files_moved: int
    archive_path: str


class Archiver:
    """Moves processed files from staging to archive bucket."""
    
    def __init__(self, config: Config) -> None:
        self.config = config
        self.storage_client = storage.Client()
    
    def run(self) -> ArchiveResult:
        """Move all files from staging to archive."""
        staging_bucket = self.storage_client.bucket(self.config.staging_bucket)
        archive_bucket = self.storage_client.bucket(self.config.archive_bucket)
        
        # Generate archive path: archive/YYYY-MM-DD/HHMM/
        now = datetime.now(timezone.utc)
        archive_prefix = f"{now.strftime('%Y-%m-%d')}/{now.strftime('%H%M')}/"
        
        files_moved = 0
        
        for blob in staging_bucket.list_blobs():
            if blob.name.endswith("/"):
                continue
            
            # Destination path
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
            archive_path=f"gs://{archive_bucket.name}/{archive_prefix}",
        )
        
        return ArchiveResult(
            files_moved=files_moved,
            archive_path=f"gs://{archive_bucket.name}/{archive_prefix}",
        )
