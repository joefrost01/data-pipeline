"""
Markets Data Pipeline Orchestrator

Runs hourly via GKE CronJob. Executes the batch pipeline:
1. Validate files in landing/
2. Run dbt build
3. Archive processed files
4. Generate extract (06:00 UTC only)
5. Push metrics
6. Write health marker
"""

import json
import sys
from datetime import datetime, timezone

import structlog
from google.cloud import storage

from orchestrator.orchestrator.config import Config
from orchestrator.orchestrator.validator import Validator
from orchestrator.orchestrator.dbt_runner import DbtRunner
from orchestrator.orchestrator.archiver import Archiver
from orchestrator.orchestrator.extract import ExtractGenerator
from orchestrator.orchestrator.metrics import MetricsClient
from orchestrator.orchestrator.control import ControlTableWriter

log = structlog.get_logger()


def write_health_marker(config: Config, run_id: str, success: bool, details: dict) -> None:
    """Write a health marker file to GCS for external monitoring.
    
    This allows external systems to check pipeline health without
    querying BigQuery or Kubernetes.
    
    Args:
        config: Pipeline configuration
        run_id: Unique run identifier
        success: Whether the pipeline run succeeded
        details: Additional details about the run
    """
    try:
        client = storage.Client()
        bucket = client.bucket(config.staging_bucket)
        
        # Write to a well-known location
        blob = bucket.blob("_health/latest.json")
        
        health_data = {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "success": success,
            "env": config.env,
            "project_id": config.project_id,
            **details,
        }
        
        blob.upload_from_string(
            json.dumps(health_data, indent=2),
            content_type="application/json",
        )
        
        # Also write timestamped marker for history
        history_blob = bucket.blob(f"_health/runs/{run_id}.json")
        history_blob.upload_from_string(
            json.dumps(health_data, indent=2),
            content_type="application/json",
        )
        
        log.debug("health_marker_written", path=f"gs://{bucket.name}/_health/latest.json")
        
    except Exception as e:
        # Don't fail the pipeline for health marker issues
        log.warning("health_marker_write_failed", error=str(e))


def main() -> int:
    """Main orchestrator entry point."""
    config = Config.from_env()
    run_start_time = datetime.now(timezone.utc)
    run_id = f"run_{run_start_time.strftime('%Y%m%d_%H%M%S')}"
    
    log.info("pipeline_started", run_id=run_id, env=config.env)
    
    metrics = MetricsClient(config)
    control = ControlTableWriter(config)
    
    # Track details for health marker
    health_details = {
        "files_validated": 0,
        "files_failed": 0,
        "models_run": 0,
        "models_failed": 0,
        "extract_generated": False,
    }
    
    try:
        # Step 1: Validate files
        log.info("step_started", step="validation")
        validator = Validator(config, control, metrics)
        validation_result = validator.run()
        
        health_details["files_validated"] = validation_result.files_passed
        health_details["files_failed"] = validation_result.files_failed
        
        if validation_result.files_failed > 0:
            log.warning(
                "validation_had_failures",
                passed=validation_result.files_passed,
                failed=validation_result.files_failed,
            )
        
        metrics.gauge("markets.files.passed", validation_result.files_passed)
        metrics.gauge("markets.files.failed", validation_result.files_failed)
        metrics.gauge("markets.rows.validated", validation_result.total_rows)
        
        # Step 2: Run dbt
        log.info("step_started", step="dbt_build")
        dbt = DbtRunner(config, control, metrics)
        dbt_result = dbt.run()
        
        health_details["models_run"] = dbt_result.models_run
        health_details["models_failed"] = dbt_result.models_failed
        health_details["tests_passed"] = dbt_result.tests_passed
        health_details["tests_failed"] = dbt_result.tests_failed
        
        if not dbt_result.success:
            log.error("dbt_build_failed", errors=dbt_result.errors[:5])
            metrics.increment("markets.pipeline.dbt_failures")
            # Continue to archive even if dbt fails - don't reprocess same files
        
        metrics.gauge("markets.rows.processed", dbt_result.rows_affected)
        metrics.gauge("markets.models.run", dbt_result.models_run)
        metrics.gauge("markets.models.failed", dbt_result.models_failed)
        metrics.gauge("markets.tests.passed", dbt_result.tests_passed)
        metrics.gauge("markets.tests.failed", dbt_result.tests_failed)
        
        # Step 3: Archive processed files
        # Pass the set of validated output paths directly to avoid race conditions
        log.info("step_started", step="archive")
        archiver = Archiver(config, validation_result.validated_output_paths)
        archive_result = archiver.run()
        
        health_details["files_archived"] = archive_result.files_moved
        
        log.info(
            "archive_complete",
            files_archived=archive_result.files_moved,
            files_skipped=archive_result.files_skipped,
            destination=archive_result.archive_path,
        )
        
        # Step 4: Generate extract (06:00 UTC only)
        current_hour = datetime.now(timezone.utc).hour
        if current_hour == config.extract_hour:
            log.info("step_started", step="extract_generation")
            extractor = ExtractGenerator(config, metrics)
            extract_result = extractor.run()
            
            health_details["extract_generated"] = True
            health_details["extract_rows"] = extract_result.row_count
            health_details["extract_path"] = extract_result.output_path
            
            log.info(
                "extract_complete",
                path=extract_result.output_path,
                rows=extract_result.row_count,
                size_bytes=extract_result.size_bytes,
            )
            metrics.gauge("markets.extract.rows", extract_result.row_count)
            metrics.gauge("markets.extract.size_bytes", extract_result.size_bytes)
        else:
            log.info(
                "extract_skipped",
                reason=f"not extract hour (current={current_hour}, expected={config.extract_hour})",
            )
        
        # Step 5: Final metrics
        metrics.increment("markets.pipeline.runs")
        
        elapsed = metrics.elapsed()
        metrics.timing("markets.pipeline.duration_seconds", elapsed)
        health_details["duration_seconds"] = elapsed
        
        # Determine overall success
        pipeline_success = dbt_result.success and validation_result.files_failed == 0
        
        log.info(
            "pipeline_complete",
            run_id=run_id,
            duration_seconds=elapsed,
            success=pipeline_success,
            files_validated=validation_result.files_passed,
            files_failed=validation_result.files_failed,
            models_run=dbt_result.models_run,
            models_failed=dbt_result.models_failed,
        )
        
        # Step 6: Write health marker
        write_health_marker(config, run_id, pipeline_success, health_details)
        
        # Flush metrics before exit
        metrics.flush()
        
        return 0 if pipeline_success else 1
        
    except Exception as e:
        log.exception("pipeline_failed", run_id=run_id, error=str(e))
        metrics.increment("markets.pipeline.failures")
        
        # Write failure health marker
        health_details["error"] = str(e)
        write_health_marker(config, run_id, False, health_details)
        
        metrics.flush()
        return 1


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    sys.exit(main())
