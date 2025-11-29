"""dbt build runner with proper error handling."""

import json
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import structlog

from orchestrator.orchestrator.config import Config
from orchestrator.orchestrator.control import ControlTableWriter
from orchestrator.orchestrator.metrics import MetricsClient

log = structlog.get_logger()


@dataclass
class DbtResult:
    """Result of dbt run."""
    success: bool
    rows_affected: int
    models_run: int
    models_failed: int
    tests_passed: int
    tests_failed: int
    errors: list[str]
    invocation_id: str


class DbtRunner:
    """Runs dbt build and logs results."""
    
    def __init__(
        self,
        config: Config,
        control: ControlTableWriter,
        metrics: MetricsClient,
    ) -> None:
        self.config = config
        self.control = control
        self.metrics = metrics
    
    def run(self, selector: str | None = None) -> DbtResult:
        """Run dbt build and return results.
        
        Args:
            selector: Optional dbt selector (e.g., 'staging+' or 'tag:curation')
        """
        invocation_id = str(uuid.uuid4())
        run_id = str(uuid.uuid4())
        
        cmd = [
            "dbt", "build",
            "--project-dir", self.config.dbt_project_dir,
            "--profiles-dir", self.config.dbt_profiles_dir,
            "--target", self.config.dbt_target,
        ]
        
        if selector:
            cmd.extend(["--select", selector])
        
        log.info("dbt_build_starting", cmd=" ".join(cmd), invocation_id=invocation_id)
        
        # Run dbt
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=self.config.dbt_project_dir,
        )
        
        # Parse run results
        run_results, parse_error = self._parse_run_results()
        
        # If we couldn't parse results and dbt exited with error, it's a failure
        if parse_error and result.returncode != 0:
            log.error(
                "dbt_build_failed_no_results",
                returncode=result.returncode,
                stderr=result.stderr[-2000:] if result.stderr else None,
                parse_error=parse_error,
            )
            
            # Log a single failed entry for the whole run
            self.control.log_dbt_run(
                run_id=run_id,
                invocation_id=invocation_id,
                model_name="_dbt_build",
                status="error",
                rows_affected=0,
                execution_time_seconds=0,
                error_message=f"dbt build failed: {result.stderr[-500:] if result.stderr else parse_error}",
            )
            
            return DbtResult(
                success=False,
                rows_affected=0,
                models_run=0,
                models_failed=1,
                tests_passed=0,
                tests_failed=0,
                errors=[parse_error, result.stderr[-500:] if result.stderr else ""],
                invocation_id=invocation_id,
            )
        
        # Process results
        models_run = 0
        models_failed = 0
        tests_passed = 0
        tests_failed = 0
        rows_affected = 0
        errors = []
        
        for model_result in run_results:
            unique_id = model_result.get("unique_id", "unknown")
            status = model_result.get("status", "unknown")
            execution_time = model_result.get("execution_time", 0)
            
            # Get rows affected from adapter response
            adapter_response = model_result.get("adapter_response", {})
            rows = adapter_response.get("rows_affected")
            if rows is None:
                # BigQuery uses different key
                rows = adapter_response.get("num_rows_affected", 0)
            
            # Determine if this is a model or test
            is_test = unique_id.startswith("test.")
            is_model = unique_id.startswith("model.") or unique_id.startswith("snapshot.")
            
            # Log to control table (models and snapshots only, not tests)
            if is_model:
                self.control.log_dbt_run(
                    run_id=run_id,
                    invocation_id=invocation_id,
                    model_name=unique_id,
                    status=status,
                    rows_affected=rows or 0,
                    execution_time_seconds=execution_time,
                    error_message=model_result.get("message") if status == "error" else None,
                    bytes_processed=adapter_response.get("bytes_processed"),
                )
            
            # Update counts
            if is_model:
                if status in ("success", "pass"):
                    models_run += 1
                    rows_affected += rows or 0
                elif status == "error":
                    models_failed += 1
                    error_msg = f"{unique_id}: {model_result.get('message', 'unknown error')}"
                    errors.append(error_msg)
            elif is_test:
                if status in ("pass", "success"):
                    tests_passed += 1
                elif status in ("fail", "error"):
                    tests_failed += 1
                    error_msg = f"{unique_id}: {model_result.get('message', 'test failed')}"
                    errors.append(error_msg)
        
        success = result.returncode == 0 and models_failed == 0
        
        log.info(
            "dbt_build_complete",
            success=success,
            models_run=models_run,
            models_failed=models_failed,
            tests_passed=tests_passed,
            tests_failed=tests_failed,
            rows_affected=rows_affected,
            invocation_id=invocation_id,
        )
        
        if not success:
            log.error(
                "dbt_build_errors",
                errors=errors[:10],  # Limit to first 10
                stderr=result.stderr[-2000:] if result.stderr else None,
            )
        
        return DbtResult(
            success=success,
            rows_affected=rows_affected,
            models_run=models_run,
            models_failed=models_failed,
            tests_passed=tests_passed,
            tests_failed=tests_failed,
            errors=errors,
            invocation_id=invocation_id,
        )
    
    def _parse_run_results(self) -> tuple[list[dict], str | None]:
        """Parse dbt run_results.json.
        
        Returns:
            Tuple of (results_list, error_message)
            If parsing fails, returns ([], error_message)
        """
        results_path = Path(self.config.dbt_project_dir) / "target" / "run_results.json"
        
        if not results_path.exists():
            error = f"run_results.json not found at {results_path}"
            log.error("run_results_not_found", path=str(results_path))
            return [], error
        
        try:
            with open(results_path) as f:
                data = json.load(f)
            
            results = data.get("results", [])
            
            if not results:
                log.warning("run_results_empty", path=str(results_path))
            
            return results, None
            
        except json.JSONDecodeError as e:
            error = f"Failed to parse run_results.json: {e}"
            log.error("run_results_parse_error", error=str(e), path=str(results_path))
            return [], error
        except Exception as e:
            error = f"Error reading run_results.json: {e}"
            log.error("run_results_read_error", error=str(e), path=str(results_path))
            return [], error
    
    def run_freshness(self) -> dict:
        """Run dbt source freshness check.
        
        Returns:
            Dict with freshness results by source
        """
        cmd = [
            "dbt", "source", "freshness",
            "--project-dir", self.config.dbt_project_dir,
            "--profiles-dir", self.config.dbt_profiles_dir,
            "--target", self.config.dbt_target,
            "--output", "json",
        ]
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=self.config.dbt_project_dir,
        )
        
        # Parse freshness results
        freshness_path = Path(self.config.dbt_project_dir) / "target" / "sources.json"
        
        try:
            if freshness_path.exists():
                with open(freshness_path) as f:
                    return json.load(f)
        except Exception as e:
            log.error("freshness_parse_error", error=str(e))
        
        return {"results": [], "error": result.stderr if result.returncode != 0 else None}
