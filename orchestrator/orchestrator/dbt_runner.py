"""dbt build runner."""

import json
import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import structlog

from orchestrator.config import Config
from orchestrator.control import ControlTableWriter
from orchestrator.metrics import MetricsClient

log = structlog.get_logger()


@dataclass
class DbtResult:
    """Result of dbt run."""
    success: bool
    rows_affected: int
    models_run: int
    models_failed: int
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
    
    def run(self) -> DbtResult:
        """Run dbt build and return results."""
        invocation_id = str(uuid.uuid4())
        
        cmd = [
            "dbt", "build",
            "--project-dir", self.config.dbt_project_dir,
            "--profiles-dir", self.config.dbt_profiles_dir,
            "--target", self.config.dbt_target,
        ]
        
        log.info("dbt_build_starting", cmd=" ".join(cmd))
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=self.config.dbt_project_dir,
        )
        
        # Parse run results from manifest
        run_results = self._parse_run_results()
        
        models_run = 0
        models_failed = 0
        rows_affected = 0
        errors = []
        
        for model_result in run_results:
            model_name = model_result.get("unique_id", "unknown")
            status = model_result.get("status", "unknown")
            execution_time = model_result.get("execution_time", 0)
            rows = model_result.get("adapter_response", {}).get("rows_affected", 0)
            
            # Log to control table
            self.control.log_dbt_run(
                run_id=str(uuid.uuid4()),
                invocation_id=invocation_id,
                model_name=model_name,
                status=status,
                rows_affected=rows or 0,
                execution_time_seconds=execution_time,
                error_message=model_result.get("message") if status == "error" else None,
            )
            
            if status == "success":
                models_run += 1
                rows_affected += rows or 0
            elif status == "error":
                models_failed += 1
                errors.append(f"{model_name}: {model_result.get('message', 'unknown error')}")
        
        success = result.returncode == 0 and models_failed == 0
        
        log.info(
            "dbt_build_complete",
            success=success,
            models_run=models_run,
            models_failed=models_failed,
            rows_affected=rows_affected,
        )
        
        if not success:
            log.error("dbt_build_errors", stderr=result.stderr[-2000:] if result.stderr else None)
        
        return DbtResult(
            success=success,
            rows_affected=rows_affected,
            models_run=models_run,
            models_failed=models_failed,
            errors=errors,
            invocation_id=invocation_id,
        )
    
    def _parse_run_results(self) -> list[dict]:
        """Parse dbt run_results.json."""
        results_path = f"{self.config.dbt_project_dir}/target/run_results.json"
        
        try:
            with open(results_path) as f:
                data = json.load(f)
                return data.get("results", [])
        except FileNotFoundError:
            log.warning("run_results_not_found", path=results_path)
            return []
        except json.JSONDecodeError as e:
            log.error("run_results_parse_error", error=str(e))
            return []
