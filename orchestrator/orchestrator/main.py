"""
Surveillance Pipeline Orchestrator

Runs hourly via GKE CronJob. Executes the batch pipeline:
1. Validate files in landing/
2. Run dbt build
3. Archive processed files
4. Generate extract (06:00 UTC only)
5. Push metrics
"""

import os
import sys
from datetime import datetime, timezone

import structlog

from orchestrator.config import Config
from orchestrator.validator import Validator
from orchestrator.dbt_runner import DbtRunner
from orchestrator.archiver import Archiver
from orchestrator.extract import ExtractGenerator
from orchestrator.metrics import MetricsClient
from orchestrator.control import ControlTableWriter

log = structlog.get_logger()


def main() -> int:
    """Main orchestrator entry point."""
    config = Config.from_env()
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    
    log.info("pipeline_started", run_id=run_id, env=config.env)
    
    metrics = MetricsClient(config)
    control = ControlTableWriter(config)
    
    try:
        # Step 1: Validate files
        log.info("step_started", step="validation")
        validator = Validator(config, control, metrics)
        validation_result = validator.run()
        
        if validation_result.files_failed > 0:
            log.warning(
                "validation_had_failures",
                passed=validation_result.files_passed,
                failed=validation_result.files_failed,
            )
        
        metrics.gauge("surveillance.files.passed", validation_result.files_passed)
        metrics.gauge("surveillance.files.failed", validation_result.files_failed)
        metrics.gauge("surveillance.rows.validated", validation_result.total_rows)
        
        # Step 2: Run dbt
        log.info("step_started", step="dbt_build")
        dbt = DbtRunner(config, control, metrics)
        dbt_result = dbt.run()
        
        if not dbt_result.success:
            log.error("dbt_build_failed", errors=dbt_result.errors)
            metrics.increment("surveillance.pipeline.dbt_failures")
            # Continue to archive even if dbt fails - don't reprocess same files
        
        metrics.gauge("surveillance.rows.processed", dbt_result.rows_affected)
        
        # Step 3: Archive processed files
        log.info("step_started", step="archive")
        archiver = Archiver(config)
        archive_result = archiver.run()
        
        log.info(
            "archive_complete",
            files_archived=archive_result.files_moved,
            destination=archive_result.archive_path,
        )
        
        # Step 4: Generate extract (06:00 UTC only)
        current_hour = datetime.now(timezone.utc).hour
        if current_hour == config.extract_hour:
            log.info("step_started", step="extract_generation")
            extractor = ExtractGenerator(config, metrics)
            extract_result = extractor.run()
            
            log.info(
                "extract_complete",
                path=extract_result.output_path,
                rows=extract_result.row_count,
                size_bytes=extract_result.size_bytes,
            )
            metrics.gauge("surveillance.extract.rows", extract_result.row_count)
        else:
            log.info("extract_skipped", reason=f"not extract hour (current={current_hour})")
        
        # Step 5: Final metrics
        metrics.increment("surveillance.pipeline.runs")
        metrics.timing("surveillance.pipeline.duration_seconds", metrics.elapsed())
        
        log.info("pipeline_complete", run_id=run_id, duration_seconds=metrics.elapsed())
        return 0
        
    except Exception as e:
        log.exception("pipeline_failed", run_id=run_id, error=str(e))
        metrics.increment("surveillance.pipeline.failures")
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
