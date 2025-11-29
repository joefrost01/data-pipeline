"""File validation against source specifications.

Validates files in landing bucket against source specs and moves them
to staging (valid) or failed (invalid) buckets.

Key safety features:
- Verifies staging upload succeeded before deleting source
- Retains source file on any failure
- Row-level quarantine preserves valid data when some rows fail
"""

import hashlib
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
import yaml
from google.cloud import storage
from google.api_core import exceptions as gcp_exceptions
from google.api_core import retry as gcp_retry

from orchestrator.config import Config
from orchestrator.control import ControlTableWriter
from orchestrator.metrics import MetricsClient
from orchestrator.parsers import get_parser

log = structlog.get_logger()

# Retry configuration for GCS operations
GCS_RETRY = gcp_retry.Retry(
    predicate=gcp_retry.if_transient_error,
    initial=1.0,
    maximum=60.0,
    multiplier=2.0,
    deadline=300.0,
)


@dataclass
class ValidationResult:
    """Result of validation run."""
    files_passed: int
    files_failed: int
    total_rows: int
    run_start_time: datetime
    validated_output_paths: set[str] = field(default_factory=set)


@dataclass
class FileValidation:
    """Result of single file validation."""
    source_name: str
    file_path: str
    passed: bool
    row_count: int
    failure_reason: str | None
    quarantined_rows: int
    output_path: str | None
    file_size_bytes: int | None
    duration_seconds: float | None


class Validator:
    """Validates files in landing bucket against source specs."""
    
    def __init__(
        self,
        config: Config,
        control: ControlTableWriter,
        metrics: MetricsClient,
    ) -> None:
        self.config = config
        self.control = control
        self.metrics = metrics
        self.storage_client = storage.Client()
        self.source_specs = self._load_source_specs()
        self.run_start_time = datetime.now(timezone.utc)
    
    def _load_source_specs(self) -> dict[str, dict]:
        """Load all source specifications from YAML files."""
        specs = {}
        specs_path = Path(self.config.source_specs_dir)
        
        for yaml_file in specs_path.rglob("*.yaml"):
            with open(yaml_file) as f:
                spec = yaml.safe_load(f)
                specs[spec["name"]] = spec
                log.debug("loaded_source_spec", name=spec["name"], path=str(yaml_file))
        
        log.info("source_specs_loaded", count=len(specs))
        return specs
    
    def run(self) -> ValidationResult:
        """Run validation on all files in landing bucket."""
        landing_bucket = self.storage_client.bucket(self.config.landing_bucket)
        staging_bucket = self.storage_client.bucket(self.config.staging_bucket)
        failed_bucket = self.storage_client.bucket(self.config.failed_bucket)
        
        files_passed = 0
        files_failed = 0
        total_rows = 0
        validated_output_paths: set[str] = set()
        
        # List all files in landing
        blobs = list(landing_bucket.list_blobs())
        log.info("files_found_in_landing", count=len(blobs))
        
        for blob in blobs:
            if blob.name.endswith("/"):
                continue  # Skip directories
            
            result = self._validate_file(blob, staging_bucket, failed_bucket)
            
            # Log to control table
            self.control.log_validation(
                run_id=str(uuid.uuid4()),
                source_name=result.source_name,
                file_path=result.file_path,
                row_count=result.row_count,
                passed=result.passed,
                failure_reason=result.failure_reason,
                quarantined_rows=result.quarantined_rows,
                output_path=result.output_path,
                file_size_bytes=result.file_size_bytes,
                duration_seconds=result.duration_seconds,
            )
            
            if result.passed:
                files_passed += 1
                total_rows += result.row_count
                if result.output_path:
                    validated_output_paths.add(result.output_path)
                self.metrics.increment("surveillance.files.passed")
            else:
                files_failed += 1
                self.metrics.increment("surveillance.files.failed")
                log.warning(
                    "file_validation_failed",
                    file=result.file_path,
                    reason=result.failure_reason,
                )
        
        return ValidationResult(
            files_passed=files_passed,
            files_failed=files_failed,
            total_rows=total_rows,
            run_start_time=self.run_start_time,
            validated_output_paths=validated_output_paths,
        )
    
    def _validate_file(
        self,
        blob: storage.Blob,
        staging_bucket: storage.Bucket,
        failed_bucket: storage.Bucket,
    ) -> FileValidation:
        """Validate a single file."""
        import time
        start_time = time.monotonic()
        
        file_path = f"gs://{blob.bucket.name}/{blob.name}"
        file_size = blob.size
        
        # Match to source spec
        source_name, spec = self._match_source_spec(blob.name)
        if spec is None:
            return self._fail_file(
                blob, failed_bucket, source_name or "unknown",
                f"No matching source spec for path: {blob.name}",
                start_time,
            )
        
        local_path = None
        try:
            # Download file with retry
            local_path = f"/tmp/{hashlib.md5(blob.name.encode()).hexdigest()}"
            blob.download_to_filename(local_path, retry=GCS_RETRY)
            
            # Parse and validate
            parser = get_parser(spec)
            rows, quarantined = parser.parse_and_validate(local_path, spec)
            
            # Check control file if configured
            if "control_file" in spec:
                expected_count = self._get_expected_row_count(blob, spec)
                if expected_count is not None and len(rows) != expected_count:
                    return self._fail_file(
                        blob, failed_bucket, source_name,
                        f"Row count mismatch: expected {expected_count}, got {len(rows)}",
                        start_time,
                    )
            
            # Convert to output format and upload to staging
            output_path = self._write_to_staging(
                rows, blob.name, spec, staging_bucket
            )
            
            # CRITICAL: Verify the staging file exists and has correct size before deleting source
            if not self._verify_staging_upload(output_path, staging_bucket, rows):
                return self._fail_file(
                    blob, failed_bucket, source_name,
                    "Staging upload verification failed - source file retained",
                    start_time,
                )
            
            # Write quarantined rows to failed bucket
            if quarantined:
                self._write_quarantined(quarantined, blob.name, failed_bucket)
            
            # Safe to delete from landing now
            try:
                blob.delete(retry=GCS_RETRY)
            except gcp_exceptions.NotFound:
                # File might have been deleted by another process - that's OK
                log.warning("source_file_already_deleted", file=blob.name)
            
            duration = time.monotonic() - start_time
            
            return FileValidation(
                source_name=source_name,
                file_path=file_path,
                passed=True,
                row_count=len(rows),
                failure_reason=None,
                quarantined_rows=len(quarantined),
                output_path=output_path,
                file_size_bytes=file_size,
                duration_seconds=duration,
            )
            
        except Exception as e:
            log.exception("validation_error", file=file_path)
            return self._fail_file(blob, failed_bucket, source_name, str(e), start_time)
        
        finally:
            # Clean up local file
            if local_path and os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    pass
    
    def _verify_staging_upload(
        self,
        output_path: str,
        staging_bucket: storage.Bucket,
        rows: list[dict],
    ) -> bool:
        """Verify that the staging file was uploaded successfully.
        
        Checks:
        1. File exists in staging bucket
        2. File size is > 0
        3. File is readable (can get metadata)
        
        Returns True if verification passes.
        """
        try:
            # Extract blob name from gs:// path
            blob_name = output_path.replace(f"gs://{staging_bucket.name}/", "")
            blob = staging_bucket.blob(blob_name)
            
            # Force reload metadata from server
            blob.reload(retry=GCS_RETRY)
            
            # Verify file exists and has content
            if blob.size is None or blob.size == 0:
                log.error(
                    "staging_verification_failed",
                    path=output_path,
                    reason="file size is 0 or None",
                )
                return False
            
            # Verify size is reasonable (at least some bytes per row)
            if len(rows) > 0:
                bytes_per_row = blob.size / len(rows)
                if bytes_per_row < 10:  # Suspiciously small
                    log.warning(
                        "staging_file_suspiciously_small",
                        path=output_path,
                        size=blob.size,
                        rows=len(rows),
                        bytes_per_row=bytes_per_row,
                    )
            
            log.debug(
                "staging_verification_passed",
                path=output_path,
                size=blob.size,
            )
            return True
            
        except gcp_exceptions.NotFound:
            log.error(
                "staging_verification_failed",
                path=output_path,
                reason="file not found",
            )
            return False
        except Exception as e:
            log.error(
                "staging_verification_error",
                path=output_path,
                error=str(e),
            )
            return False
    
    def _match_source_spec(self, blob_name: str) -> tuple[str | None, dict | None]:
        """Match a blob name to a source specification."""
        for name, spec in self.source_specs.items():
            pattern = spec["source"]["path_pattern"]
            # Simple glob-style matching
            if self._matches_pattern(blob_name, pattern):
                return name, spec
        return None, None
    
    def _matches_pattern(self, path: str, pattern: str) -> bool:
        """Check if path matches glob pattern."""
        import fnmatch
        # Remove bucket prefix from pattern if present
        pattern = pattern.replace("landing/", "")
        return fnmatch.fnmatch(path, pattern)
    
    def _get_expected_row_count(self, blob: storage.Blob, spec: dict) -> int | None:
        """Get expected row count from control file."""
        control_config = spec.get("control_file", {})
        control_type = control_config.get("type")
        
        # Get the directory path from the blob name
        blob_dir = str(Path(blob.name).parent)
        blob_base = Path(blob.name).stem  # filename without extension
        
        if control_type == "sidecar_xml":
            # Look for .ctrl or _ctrl.xml file
            ctrl_pattern = control_config.get("pattern", "{filename}_ctrl.xml")
            ctrl_name = ctrl_pattern.replace("{filename}", blob_base)
            # Preserve directory path
            if blob_dir and blob_dir != ".":
                ctrl_name = f"{blob_dir}/{ctrl_name}"
            ctrl_blob = blob.bucket.blob(ctrl_name)
            
            try:
                if ctrl_blob.exists():
                    import xml.etree.ElementTree as ET
                    ctrl_content = ctrl_blob.download_as_text(retry=GCS_RETRY)
                    root = ET.fromstring(ctrl_content)
                    xpath = control_config.get("xpath_row_count", "RecordCount")
                    # Handle xpath with leading /
                    xpath = xpath.lstrip("/").replace("/", "./")
                    element = root.find(xpath)
                    if element is not None and element.text:
                        return int(element.text)
            except Exception as e:
                log.warning("control_file_parse_error", error=str(e), ctrl_path=ctrl_name)
        
        elif control_type == "sidecar_csv":
            ctrl_pattern = control_config.get("pattern", "{filename}.ctrl")
            ctrl_name = ctrl_pattern.replace("{filename}", blob_base)
            # Preserve directory path
            if blob_dir and blob_dir != ".":
                ctrl_name = f"{blob_dir}/{ctrl_name}"
            ctrl_blob = blob.bucket.blob(ctrl_name)
            
            try:
                if ctrl_blob.exists():
                    import csv
                    from io import StringIO
                    ctrl_content = ctrl_blob.download_as_text(retry=GCS_RETRY)
                    reader = csv.DictReader(StringIO(ctrl_content))
                    row = next(reader, None)
                    if row:
                        field = control_config.get("row_count_field", "record_count")
                        return int(row.get(field, 0))
            except Exception as e:
                log.warning("control_file_parse_error", error=str(e), ctrl_path=ctrl_name)
        
        return None
    
    def _write_to_staging(
        self,
        rows: list[dict],
        original_name: str,
        spec: dict,
        staging_bucket: storage.Bucket,
    ) -> str:
        """Write validated rows to staging bucket as Parquet."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        
        # Generate output path preserving directory structure
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        original_path = Path(original_name)
        base_name = original_path.stem
        parent = original_path.parent
        output_name = f"{parent}/{base_name}_{timestamp}.parquet"
        
        # Convert to Parquet
        table = pa.Table.from_pylist(rows)
        local_parquet = f"/tmp/{hashlib.md5(output_name.encode()).hexdigest()}.parquet"
        pq.write_table(table, local_parquet)
        
        try:
            # Upload with retry
            output_blob = staging_bucket.blob(output_name)
            output_blob.upload_from_filename(
                local_parquet,
                timeout=120,  # 2 minute timeout for large files
                retry=GCS_RETRY,
            )
            
            return f"gs://{staging_bucket.name}/{output_name}"
        finally:
            # Clean up local file
            if os.path.exists(local_parquet):
                os.remove(local_parquet)
    
    def _write_quarantined(
        self,
        quarantined: list[dict],
        original_name: str,
        failed_bucket: storage.Bucket,
    ) -> None:
        """Write quarantined rows to failed bucket."""
        import json
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        output_name = f"quarantined/{original_name}_{timestamp}.jsonl"
        
        content = "\n".join(json.dumps(row, default=str) for row in quarantined)
        
        blob = failed_bucket.blob(output_name)
        blob.upload_from_string(content, retry=GCS_RETRY)
        
        log.info(
            "quarantined_rows_written",
            count=len(quarantined),
            path=f"gs://{failed_bucket.name}/{output_name}",
        )
    
    def _fail_file(
        self,
        blob: storage.Blob,
        failed_bucket: storage.Bucket,
        source_name: str,
        reason: str,
        start_time: float,
    ) -> FileValidation:
        """Move file to failed bucket and return failure result."""
        import time
        
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        failed_name = f"{blob.name}_{timestamp}"
        
        try:
            # Copy to failed bucket (keep original as backup)
            failed_blob = failed_bucket.blob(failed_name)
            failed_blob.rewrite(blob, retry=GCS_RETRY)
            
            # Write error log
            error_log = failed_bucket.blob(f"{failed_name}.error.txt")
            error_content = f"""Failure reason: {reason}
Timestamp: {timestamp}
Source file: gs://{blob.bucket.name}/{blob.name}
Source name: {source_name}
"""
            error_log.upload_from_string(error_content, retry=GCS_RETRY)
            
            # Delete from landing only after successful copy to failed
            blob.delete(retry=GCS_RETRY)
            
        except Exception as e:
            log.error(
                "failed_to_move_to_failed_bucket",
                file=blob.name,
                error=str(e),
            )
            # Don't delete original if we couldn't copy to failed
        
        duration = time.monotonic() - start_time
        
        return FileValidation(
            source_name=source_name,
            file_path=f"gs://{blob.bucket.name}/{blob.name}",
            passed=False,
            row_count=0,
            failure_reason=reason,
            quarantined_rows=0,
            output_path=f"gs://{failed_bucket.name}/{failed_name}",
            file_size_bytes=blob.size,
            duration_seconds=duration,
        )
