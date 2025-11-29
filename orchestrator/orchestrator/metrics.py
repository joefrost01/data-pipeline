"""Metrics client for Dynatrace."""

import time
from pathlib import Path
from typing import Any

import structlog

from orchestrator.config import Config

log = structlog.get_logger()


class MetricsClient:
    """Push metrics to Dynatrace."""
    
    def __init__(self, config: Config) -> None:
        self.config = config
        self.start_time = time.monotonic()
        self._buffer: list[str] = []
        self._token: str | None = None
    
    def _get_token(self) -> str | None:
        """Load Dynatrace token from file."""
        if self._token is not None:
            return self._token
        
        token_path = Path(self.config.dynatrace_token_path)
        if token_path.exists():
            self._token = token_path.read_text().strip()
            return self._token
        
        log.debug("dynatrace_token_not_found", path=str(token_path))
        return None
    
    def increment(self, metric: str, value: int = 1, dimensions: dict[str, Any] | None = None) -> None:
        """Increment a counter metric."""
        self._record(metric, value, "count", dimensions)
    
    def gauge(self, metric: str, value: float, dimensions: dict[str, Any] | None = None) -> None:
        """Record a gauge metric."""
        self._record(metric, value, "gauge", dimensions)
    
    def timing(self, metric: str, value: float, dimensions: dict[str, Any] | None = None) -> None:
        """Record a timing metric in seconds."""
        self._record(metric, value, "gauge", dimensions)
    
    def elapsed(self) -> float:
        """Return elapsed time since client creation."""
        return time.monotonic() - self.start_time
    
    def _record(
        self,
        metric: str,
        value: float,
        metric_type: str,
        dimensions: dict[str, Any] | None = None,
    ) -> None:
        """Record a metric to the buffer."""
        dims = {"env": self.config.env}
        if dimensions:
            dims.update(dimensions)
        
        dim_str = ",".join(f"{k}={v}" for k, v in dims.items())
        line = f"{metric},{dim_str} {metric_type}={value}"
        self._buffer.append(line)
        
        log.debug("metric_recorded", metric=metric, value=value, type=metric_type)
    
    def flush(self) -> None:
        """Send buffered metrics to Dynatrace."""
        if not self._buffer:
            return
        
        token = self._get_token()
        if not token or not self.config.dynatrace_endpoint:
            log.debug("metrics_flush_skipped", reason="no endpoint or token configured")
            self._buffer.clear()
            return
        
        try:
            import httpx
            
            payload = "\n".join(self._buffer)
            
            response = httpx.post(
                f"{self.config.dynatrace_endpoint}/api/v2/metrics/ingest",
                headers={
                    "Authorization": f"Api-Token {token}",
                    "Content-Type": "text/plain",
                },
                content=payload,
                timeout=10,
            )
            
            if response.status_code == 202:
                log.info("metrics_flushed", count=len(self._buffer))
            else:
                log.error(
                    "metrics_flush_failed",
                    status=response.status_code,
                    body=response.text[:500],
                )
        except Exception as e:
            log.warning("metrics_flush_error", error=str(e))
        finally:
            self._buffer.clear()
    
    def __del__(self) -> None:
        """Flush on destruction."""
        try:
            self.flush()
        except Exception:
            pass  # Don't raise in destructor
