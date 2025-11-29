"""File validation against source specifications."""

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog
import yaml
from google.cloud import storage

from orchestrator.config import Config
from orchestrator.control import ControlTableWriter
from orchestrator.metrics import MetricsClient
from orchestrator.parsers import get_parser

log = structlog.get_logger()


@dataclass
class ValidationResult:
    """Result of validation run."""
    files_passed: int
    files_failed: int
    total_rows: int


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
            )
            
            if result.passed:
                files_passed += 1
                total_rows += result.row_count
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
        )
    
    def _validate_file(
        self,
        blob: storage.Blob,
        staging_bucket: storage.Bucket,
        failed_bucket: storage.Bucket,
    ) -> FileValidation:
        """Validate a single file."""
        file_path = f"gs://{blob.bucket.name}/{blob.name}"
        
        # Match to source spec
        source_name, spec = self._match_source_spec(blob.name)
        if spec is None:
            return self._fail_file(
                blob, failed_bucket, source_name or "unknown",
                f"No matching source spec for path: {blob.name}"
            )
        
        try:
            # Download file
            local_path = f"/tmp/{hashlib.md5(blob.name.encode()).hexdigest()}"
            blob.download_to_filename(local_path)
            
            # Parse and validate
            parser = get_parser(spec)
            rows, quarantined = parser.parse_and_validate(local_path, spec)
            
            # Check control file if configured
            if "control_file" in spec:
                expected_count = self._get_expected_row_count(blob, spec)
                if expected_count is not None and len(rows) != expected_count:
                    return self._fail_file(
                        blob, failed_bucket, source_name,
                        f"Row count mismatch: expected {expected_count}, got {len(rows)}"
                    )
            
            # Convert to output format and upload to staging
            output_path = self._write_to_staging(
                rows, blob.name, spec, staging_bucket
            )
            
            # Write quarantined rows to failed bucket
            if quarantined:
                self._write_quarantined(quarantined, blob.name, failed_bucket)
            
            # Delete from landing
            blob.delete()
            
            return FileValidation(
                source_name=source_name,
                file_path=file_path,
                passed=True,
                row_count=len(rows),
                failure_reason=None,
                quarantined_rows=len(quarantined),
                output_path=output_path,
            )
            
        except Exception as e:
            log.exception("validation_error", file=file_path)
            return self._fail_file(blob, failed_bucket, source_name, str(e))
    
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
        
        if control_type == "sidecar_xml":
            # Look for .ctrl or _ctrl.xml file
            ctrl_name = blob.name.rsplit(".", 1)[0] + "_ctrl.xml"
            ctrl_blob = blob.bucket.blob(ctrl_name)
            
            if ctrl_blob.exists():
                import xml.etree.ElementTree as ET
                ctrl_content = ctrl_blob.download_as_text()
                root = ET.fromstring(ctrl_content)
                xpath = control_config.get("xpath_row_count")
                element = root.find(xpath.replace("/", "./"))
                if element is not None and element.text:
                    return int(element.text)
        
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
        
        # Generate output path
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_name = Path(original_name).stem
        output_name = f"{Path(original_name).parent}/{base_name}_{timestamp}.parquet"
        
        # Convert to Parquet
        table = pa.Table.from_pylist(rows)
        local_parquet = f"/tmp/{hashlib.md5(output_name.encode()).hexdigest()}.parquet"
        pq.write_table(table, local_parquet)
        
        # Upload
        output_blob = staging_bucket.blob(output_name)
        output_blob.upload_from_filename(local_parquet)
        
        return f"gs://{staging_bucket.name}/{output_name}"
    
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
        
        content = "\n".join(json.dumps(row) for row in quarantined)
        
        blob = failed_bucket.blob(output_name)
        blob.upload_from_string(content)
    
    def _fail_file(
        self,
        blob: storage.Blob,
        failed_bucket: storage.Bucket,
        source_name: str,
        reason: str,
    ) -> FileValidation:
        """Move file to failed bucket and return failure result."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        failed_name = f"{blob.name}_{timestamp}"
        
        # Copy to failed bucket
        failed_blob = failed_bucket.blob(failed_name)
        failed_blob.rewrite(blob)
        
        # Write error log
        error_log = failed_bucket.blob(f"{failed_name}.error.txt")
        error_log.upload_from_string(f"Failure reason: {reason}\nTimestamp: {timestamp}")
        
        # Delete from landing
        blob.delete()
        
        return FileValidation(
            source_name=source_name,
            file_path=f"gs://{blob.bucket.name}/{blob.name}",
            passed=False,
            row_count=0,
            failure_reason=reason,
            quarantined_rows=0,
            output_path=f"gs://{failed_bucket.name}/{failed_name}",
        )
