"""Regulatory Reporter Service.

Low-latency service for submitting regulatory reports within 15-minute SLA.
Features:
- In-memory cache with periodic refresh
- Cache-aside pattern for cache misses
- Admin endpoint for manual cache refresh
- Idempotent submissions
- Retry with exponential backoff
"""

import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog
from flask import Flask, jsonify, request
from google.cloud import bigquery

log = structlog.get_logger()

app = Flask(__name__)

# Namespace for deterministic event ID generation (must match dbt macro)
MARKETS_NAMESPACE = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'


@dataclass
class CacheConfig:
    """Configuration for reference data cache."""
    refresh_interval_seconds: int = 300  # 5 minutes
    cache_miss_timeout_seconds: float = 5.0  # Timeout for cache-aside lookups
    stale_threshold_seconds: int = 600  # Consider cache stale after 10 min


@dataclass
class RetryConfig:
    """Configuration for submission retries."""
    max_attempts: int = 5
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 16.0
    exponential_base: float = 2.0


@dataclass
class ReferenceCache:
    """In-memory cache for reference data.
    
    IMPORTANT: Each lookup type (by ID vs by name) uses a SEPARATE cache dict.
    This prevents key collisions when the same entity might be looked up
    by different keys.
    """
    traders: dict[str, dict] = field(default_factory=dict)
    counterparties_by_id: dict[str, dict] = field(default_factory=dict)
    counterparties_by_name: dict[str, dict] = field(default_factory=dict)
    instruments: dict[str, dict] = field(default_factory=dict)
    books: dict[str, dict] = field(default_factory=dict)
    last_refresh: datetime | None = None
    refresh_in_progress: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)


class RegulatoryReporter:
    """Main regulatory reporting service."""
    
    def __init__(self) -> None:
        self.project_id = os.environ["PROJECT_ID"]
        self.regulator_api_url = os.environ["REGULATOR_API_URL"]
        self.regulator_api_key = os.environ.get("REGULATOR_API_KEY", "")
        
        self.cache_config = CacheConfig(
            refresh_interval_seconds=int(
                os.environ.get("CACHE_REFRESH_SECONDS", "300")
            ),
        )
        
        self.retry_config = RetryConfig(
            max_attempts=int(os.environ.get("RETRY_MAX_ATTEMPTS", "5")),
            initial_delay_seconds=float(os.environ.get("RETRY_INITIAL_DELAY", "1.0")),
            max_delay_seconds=float(os.environ.get("RETRY_MAX_DELAY", "16.0")),
        )
        
        self.cache = ReferenceCache()
        self.bq_client = bigquery.Client()
        self.http_client = httpx.Client(timeout=30.0)
        
        # Start background refresh thread
        self._shutdown = threading.Event()
        self._refresh_thread = threading.Thread(
            target=self._background_refresh_loop,
            daemon=True,
        )
        self._refresh_thread.start()
        
        # Initial cache load
        self.refresh_cache()
        
        log.info(
            "reporter_initialised",
            refresh_interval=self.cache_config.refresh_interval_seconds,
            max_retry_attempts=self.retry_config.max_attempts,
        )
    
    def refresh_cache(self, force: bool = False) -> dict[str, Any]:
        """Refresh all reference data caches from BigQuery.
        
        Args:
            force: If True, refresh even if another refresh is in progress
            
        Returns:
            Dict with refresh statistics
        """
        with self.cache._lock:
            if self.cache.refresh_in_progress and not force:
                return {"status": "skipped", "reason": "refresh_in_progress"}
            self.cache.refresh_in_progress = True
        
        start_time = time.monotonic()
        stats = {"traders": 0, "counterparties": 0, "instruments": 0, "books": 0}
        
        try:
            # Refresh traders (current snapshot)
            traders_query = """
                SELECT trader_id, trader_name, desk_id, compliance_officer
                FROM snapshots.traders_snapshot
                WHERE dbt_valid_to IS NULL
            """
            traders = {}
            for row in self.bq_client.query(traders_query).result():
                traders[row.trader_id] = dict(row)
            
            # Refresh counterparties - use SEPARATE dicts for ID and name lookup
            # This is critical to avoid key collisions
            counterparties_query = """
                SELECT counterparty_id, counterparty_name, lei, country
                FROM snapshots.counterparties_snapshot
                WHERE dbt_valid_to IS NULL
            """
            counterparties_by_id = {}
            counterparties_by_name = {}
            for row in self.bq_client.query(counterparties_query).result():
                row_dict = dict(row)
                counterparties_by_id[row.counterparty_id] = row_dict
                # Only add to name lookup if name exists and doesn't collide
                # First one wins to prevent silent overwrites
                if row.counterparty_name and row.counterparty_name not in counterparties_by_name:
                    counterparties_by_name[row.counterparty_name] = row_dict
            
            # Refresh instruments
            instruments_query = """
                SELECT instrument_id, symbol, isin, asset_class, currency
                FROM curation.dim_instrument
            """
            instruments = {}
            for row in self.bq_client.query(instruments_query).result():
                instruments[row.instrument_id] = dict(row)
            
            # Refresh books
            books_query = """
                SELECT book_id, book_name, desk_id, legal_entity
                FROM snapshots.books_snapshot
                WHERE dbt_valid_to IS NULL
            """
            books = {}
            for row in self.bq_client.query(books_query).result():
                books[row.book_id] = dict(row)
            
            # Atomic swap
            with self.cache._lock:
                self.cache.traders = traders
                self.cache.counterparties_by_id = counterparties_by_id
                self.cache.counterparties_by_name = counterparties_by_name
                self.cache.instruments = instruments
                self.cache.books = books
                self.cache.last_refresh = datetime.now(timezone.utc)
            
            stats = {
                "traders": len(traders),
                "counterparties": len(counterparties_by_id),
                "counterparties_by_name": len(counterparties_by_name),
                "instruments": len(instruments),
                "books": len(books),
            }
            
            duration = time.monotonic() - start_time
            log.info("cache_refreshed", duration_seconds=duration, **stats)
            
            return {
                "status": "success",
                "duration_seconds": duration,
                "counts": stats,
                "refreshed_at": self.cache.last_refresh.isoformat(),
            }
            
        except Exception as e:
            log.exception("cache_refresh_failed", error=str(e))
            return {"status": "error", "error": str(e)}
        
        finally:
            with self.cache._lock:
                self.cache.refresh_in_progress = False
    
    def _background_refresh_loop(self) -> None:
        """Background thread for periodic cache refresh."""
        while not self._shutdown.is_set():
            time.sleep(self.cache_config.refresh_interval_seconds)
            if not self._shutdown.is_set():
                self.refresh_cache()
    
    def _lookup_trader_with_fallback(self, trader_id: str) -> dict | None:
        """Look up trader with cache-aside pattern."""
        # Check cache first
        if trader_id in self.cache.traders:
            return self.cache.traders[trader_id]
        
        # Cache miss - query BigQuery directly
        log.warning("cache_miss", table="traders", key=trader_id)
        
        try:
            query = """
                SELECT trader_id, trader_name, desk_id, compliance_officer
                FROM snapshots.traders_snapshot
                WHERE trader_id = @key AND dbt_valid_to IS NULL
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("key", "STRING", trader_id)
                ]
            )
            
            results = list(self.bq_client.query(
                query,
                job_config=job_config,
                timeout=self.cache_config.cache_miss_timeout_seconds,
            ).result())
            
            if results:
                data = dict(results[0])
                self.cache.traders[trader_id] = data
                return data
            
            return None
            
        except Exception as e:
            log.error("cache_fallback_failed", table="traders", key=trader_id, error=str(e))
            return None
    
    def _lookup_counterparty_by_id_with_fallback(self, counterparty_id: str) -> dict | None:
        """Look up counterparty by ID with cache-aside pattern."""
        if counterparty_id in self.cache.counterparties_by_id:
            return self.cache.counterparties_by_id[counterparty_id]
        
        log.warning("cache_miss", table="counterparties", key=counterparty_id, lookup_type="id")
        
        try:
            query = """
                SELECT counterparty_id, counterparty_name, lei, country
                FROM snapshots.counterparties_snapshot
                WHERE counterparty_id = @key AND dbt_valid_to IS NULL
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("key", "STRING", counterparty_id)
                ]
            )
            
            results = list(self.bq_client.query(
                query,
                job_config=job_config,
                timeout=self.cache_config.cache_miss_timeout_seconds,
            ).result())
            
            if results:
                data = dict(results[0])
                # Update ONLY the by-ID cache
                self.cache.counterparties_by_id[counterparty_id] = data
                return data
            
            return None
            
        except Exception as e:
            log.error("cache_fallback_failed", table="counterparties", key=counterparty_id, error=str(e))
            return None
    
    def _lookup_counterparty_by_name_with_fallback(self, counterparty_name: str) -> dict | None:
        """Look up counterparty by name with cache-aside pattern."""
        if counterparty_name in self.cache.counterparties_by_name:
            return self.cache.counterparties_by_name[counterparty_name]
        
        log.warning("cache_miss", table="counterparties", key=counterparty_name, lookup_type="name")
        
        try:
            query = """
                SELECT counterparty_id, counterparty_name, lei, country
                FROM snapshots.counterparties_snapshot
                WHERE counterparty_name = @key AND dbt_valid_to IS NULL
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("key", "STRING", counterparty_name)
                ]
            )
            
            results = list(self.bq_client.query(
                query,
                job_config=job_config,
                timeout=self.cache_config.cache_miss_timeout_seconds,
            ).result())
            
            if results:
                data = dict(results[0])
                # Update ONLY the by-name cache
                self.cache.counterparties_by_name[counterparty_name] = data
                return data
            
            return None
            
        except Exception as e:
            log.error("cache_fallback_failed", table="counterparties", key=counterparty_name, error=str(e))
            return None
    
    def _lookup_instrument_with_fallback(self, instrument_id: str) -> dict | None:
        """Look up instrument with cache-aside pattern."""
        if instrument_id in self.cache.instruments:
            return self.cache.instruments[instrument_id]
        
        log.warning("cache_miss", table="instruments", key=instrument_id)
        
        try:
            query = """
                SELECT instrument_id, symbol, isin, asset_class, currency
                FROM curation.dim_instrument
                WHERE instrument_id = @key
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("key", "STRING", instrument_id)
                ]
            )
            
            results = list(self.bq_client.query(
                query,
                job_config=job_config,
                timeout=self.cache_config.cache_miss_timeout_seconds,
            ).result())
            
            if results:
                data = dict(results[0])
                self.cache.instruments[instrument_id] = data
                return data
            
            return None
            
        except Exception as e:
            log.error("cache_fallback_failed", table="instruments", key=instrument_id, error=str(e))
            return None
    
    def get_trader(self, trader_id: str) -> dict | None:
        """Get trader reference data with cache-aside fallback."""
        return self._lookup_trader_with_fallback(trader_id)
    
    def get_counterparty(self, counterparty_id: str) -> dict | None:
        """Get counterparty reference data by ID with cache-aside fallback."""
        return self._lookup_counterparty_by_id_with_fallback(counterparty_id)
    
    def get_counterparty_by_name(self, counterparty_name: str) -> dict | None:
        """Get counterparty reference data by name with cache-aside fallback."""
        return self._lookup_counterparty_by_name_with_fallback(counterparty_name)
    
    def get_instrument(self, instrument_id: str) -> dict | None:
        """Get instrument reference data with cache-aside fallback."""
        return self._lookup_instrument_with_fallback(instrument_id)
    
    def _submit_with_retry(
        self, 
        event_id: str, 
        enriched: dict
    ) -> tuple[bool, dict[str, Any]]:
        """Submit to regulator with exponential backoff retry.
        
        Args:
            event_id: Deterministic event ID for idempotency
            enriched: Enriched event payload
            
        Returns:
            Tuple of (success, result_dict)
        """
        delay = self.retry_config.initial_delay_seconds
        last_error = None
        
        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                response = self.http_client.post(
                    self.regulator_api_url,
                    json=enriched,
                    headers={
                        "Authorization": f"Bearer {self.regulator_api_key}",
                        "Content-Type": "application/json",
                        "X-Idempotency-Key": event_id,
                    },
                )
                
                # Success
                if response.status_code in (200, 201, 202):
                    result = response.json()
                    return True, {
                        "regulator_reference": result.get("reference", result.get("id")),
                        "response": result,
                    }
                
                # Non-retryable client errors
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    return False, {
                        "error": f"HTTP {response.status_code}: {response.text[:200]}",
                        "retryable": False,
                    }
                
                # Retryable errors (5xx, 429)
                last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                log.warning(
                    "submission_retry",
                    event_id=event_id,
                    attempt=attempt,
                    status_code=response.status_code,
                    delay=delay,
                )
                
            except httpx.TimeoutException as e:
                last_error = f"Timeout: {str(e)}"
                log.warning(
                    "submission_timeout_retry",
                    event_id=event_id,
                    attempt=attempt,
                    delay=delay,
                )
            except httpx.RequestError as e:
                last_error = f"Request error: {str(e)}"
                log.warning(
                    "submission_request_error_retry",
                    event_id=event_id,
                    attempt=attempt,
                    error=str(e),
                    delay=delay,
                )
            
            # Don't sleep after last attempt
            if attempt < self.retry_config.max_attempts:
                time.sleep(delay)
                delay = min(
                    delay * self.retry_config.exponential_base,
                    self.retry_config.max_delay_seconds,
                )
        
        return False, {
            "error": f"Max retries exceeded. Last error: {last_error}",
            "retryable": True,
            "attempts": self.retry_config.max_attempts,
        }
    
    def submit_event(self, event: dict) -> dict[str, Any]:
        """Submit a regulatory event.
        
        Args:
            event: Event data including trade details
            
        Returns:
            Submission result with status and regulator reference
        """
        event_id = self._generate_event_id(event)
        event_timestamp = datetime.fromisoformat(
            event.get("event_timestamp", datetime.now(timezone.utc).isoformat())
        )
        
        # Check for duplicate submission
        if self._is_duplicate(event_id):
            log.info("duplicate_submission_skipped", event_id=event_id)
            return {
                "status": "duplicate",
                "event_id": event_id,
                "message": "Event already submitted",
            }
        
        # Enrich with reference data
        enriched = self._enrich_event(event)
        
        if enriched is None:
            return {
                "status": "error",
                "event_id": event_id,
                "message": "Failed to enrich event - missing reference data",
            }
        
        # Submit to regulator with retry
        submission_start = time.monotonic()
        success, result = self._submit_with_retry(event_id, enriched)
        submission_latency = time.monotonic() - submission_start
        
        total_latency = (
            datetime.now(timezone.utc) - event_timestamp
        ).total_seconds()
        
        if success:
            regulator_reference = result.get("regulator_reference")
            
            # Log to audit table
            self._log_submission(
                event_id=event_id,
                event_timestamp=event_timestamp,
                regulator_reference=regulator_reference,
                submission_latency=submission_latency,
                total_latency=total_latency,
                status="SUBMITTED",
                payload_hash=self._hash_payload(enriched),
                retry_count=0,
            )
            
            log.info(
                "submission_successful",
                event_id=event_id,
                regulator_reference=regulator_reference,
                latency_seconds=total_latency,
            )
            
            return {
                "status": "success",
                "event_id": event_id,
                "regulator_reference": regulator_reference,
                "latency_seconds": total_latency,
            }
        else:
            # Log failure to dead letter
            self._log_dead_letter(
                event_id=event_id,
                event_timestamp=event_timestamp,
                failure_reason=result.get("error", "Unknown error"),
                retry_count=result.get("attempts", self.retry_config.max_attempts),
                event_payload=event,
            )
            
            log.error(
                "submission_failed",
                event_id=event_id,
                error=result.get("error"),
            )
            
            return {
                "status": "error",
                "event_id": event_id,
                "message": result.get("error"),
            }
    
    def _generate_event_id(self, event: dict) -> str:
        """Generate deterministic event ID for idempotency.
        
        Uses the same format as the dbt generate_event_id macro:
        MD5(namespace:event:domain:source_system:source_event_id)
        """
        domain = event.get("domain", "markets")
        source_system = event.get("source_system", "UNKNOWN")
        source_event_id = event.get("source_event_id", event.get("trade_id", ""))
        
        # Match the dbt macro format exactly
        id_string = f"{MARKETS_NAMESPACE}:event:{domain}:{source_system}:{source_event_id}"
        return hashlib.md5(id_string.encode()).hexdigest()
    
    def _is_duplicate(self, event_id: str) -> bool:
        """Check if event has already been submitted."""
        query = """
            SELECT 1 FROM control.regulatory_submissions
            WHERE event_id = @event_id
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("event_id", "STRING", event_id)
            ]
        )
        results = list(self.bq_client.query(query, job_config=job_config).result())
        return len(results) > 0
    
    def _enrich_event(self, event: dict) -> dict | None:
        """Enrich event with reference data."""
        enriched = event.copy()
        
        # Enrich trader
        trader_id = event.get("trader_id")
        if trader_id:
            trader = self.get_trader(trader_id)
            if trader:
                enriched["trader_name"] = trader.get("trader_name")
                enriched["desk_id"] = trader.get("desk_id")
                enriched["compliance_officer"] = trader.get("compliance_officer")
            else:
                log.warning("trader_not_found", trader_id=trader_id)
                # Don't fail - trader might be new
        
        # Enrich counterparty - try ID first, then name
        counterparty_id = event.get("counterparty_id")
        counterparty_name = event.get("counterparty_name")
        counterparty = None
        
        if counterparty_id:
            counterparty = self.get_counterparty(counterparty_id)
        elif counterparty_name:
            counterparty = self.get_counterparty_by_name(counterparty_name)
        
        if counterparty:
            enriched["counterparty_name"] = counterparty.get("counterparty_name")
            enriched["counterparty_lei"] = counterparty.get("lei")
        
        # Enrich instrument
        instrument_id = event.get("instrument_id")
        if instrument_id:
            instrument = self.get_instrument(instrument_id)
            if instrument:
                enriched["instrument_symbol"] = instrument.get("symbol")
                enriched["isin"] = instrument.get("isin")
                enriched["asset_class"] = instrument.get("asset_class")
        
        return enriched
    
    def _hash_payload(self, payload: dict) -> str:
        """Generate SHA256 hash of payload for audit."""
        payload_str = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(payload_str.encode()).hexdigest()
    
    def _log_submission(
        self,
        event_id: str,
        event_timestamp: datetime,
        regulator_reference: str | None,
        submission_latency: float,
        total_latency: float,
        status: str,
        payload_hash: str,
        retry_count: int = 0,
    ) -> None:
        """Log submission to audit table."""
        row = {
            "submission_id": str(uuid.uuid4()),
            "event_id": event_id,
            "event_timestamp": event_timestamp.isoformat(),
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "regulator_reference": regulator_reference,
            "submission_latency_seconds": submission_latency,
            "status": status,
            "report_type": "TRADE",
            "report_payload_hash": payload_hash,
            "retry_count": retry_count,
        }
        
        errors = self.bq_client.insert_rows_json(
            "control.regulatory_submissions", [row]
        )
        
        if errors:
            log.error("audit_log_failed", errors=errors)
    
    def _log_dead_letter(
        self,
        event_id: str,
        event_timestamp: datetime,
        failure_reason: str,
        retry_count: int,
        event_payload: dict,
    ) -> None:
        """Log failed submission to dead letter table."""
        row = {
            "dead_letter_id": str(uuid.uuid4()),
            "event_id": event_id,
            "event_timestamp": event_timestamp.isoformat(),
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "failure_reason": failure_reason,
            "retry_count": retry_count,
            "last_error": failure_reason,
            "event_payload": json.dumps(event_payload, default=str),
        }
        
        errors = self.bq_client.insert_rows_json(
            "control.regulatory_dead_letter", [row]
        )
        
        if errors:
            log.error("dead_letter_log_failed", errors=errors)
    
    def get_cache_status(self) -> dict[str, Any]:
        """Get cache status for health checks."""
        with self.cache._lock:
            last_refresh = self.cache.last_refresh
            is_stale = False
            
            if last_refresh:
                age_seconds = (
                    datetime.now(timezone.utc) - last_refresh
                ).total_seconds()
                is_stale = age_seconds > self.cache_config.stale_threshold_seconds
            
            return {
                "last_refresh": last_refresh.isoformat() if last_refresh else None,
                "is_stale": is_stale,
                "refresh_in_progress": self.cache.refresh_in_progress,
                "counts": {
                    "traders": len(self.cache.traders),
                    "counterparties_by_id": len(self.cache.counterparties_by_id),
                    "counterparties_by_name": len(self.cache.counterparties_by_name),
                    "instruments": len(self.cache.instruments),
                    "books": len(self.cache.books),
                },
            }
    
    def shutdown(self) -> None:
        """Graceful shutdown."""
        self._shutdown.set()
        self.http_client.close()


# Global reporter instance
reporter: RegulatoryReporter | None = None


def get_reporter() -> RegulatoryReporter:
    """Get or create the global reporter instance."""
    global reporter
    if reporter is None:
        reporter = RegulatoryReporter()
    return reporter


# Flask routes

@app.route("/health", methods=["GET"])
def health():
    """Health check endpoint."""
    r = get_reporter()
    cache_status = r.get_cache_status()
    
    healthy = not cache_status["is_stale"]
    
    return jsonify({
        "status": "healthy" if healthy else "degraded",
        "cache": cache_status,
    }), 200 if healthy else 503


@app.route("/submit", methods=["POST"])
def submit():
    """Submit a regulatory event."""
    event = request.get_json()
    if not event:
        return jsonify({"error": "No event data provided"}), 400
    
    r = get_reporter()
    result = r.submit_event(event)
    
    status_code = 200 if result["status"] == "success" else 500
    if result["status"] == "duplicate":
        status_code = 200  # Duplicate is not an error
    
    return jsonify(result), status_code


@app.route("/admin/refresh-cache", methods=["POST"])
def refresh_cache():
    """Admin endpoint to manually refresh cache."""
    force = request.args.get("force", "false").lower() == "true"
    
    r = get_reporter()
    result = r.refresh_cache(force=force)
    
    status_code = 200 if result["status"] == "success" else 500
    return jsonify(result), status_code


@app.route("/admin/cache-status", methods=["GET"])
def cache_status():
    """Get current cache status."""
    r = get_reporter()
    return jsonify(r.get_cache_status()), 200


def create_app():
    """Create Flask app for production WSGI server (e.g., gunicorn)."""
    # Ensure reporter is initialised
    get_reporter()
    return app


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    
    # NOTE: This is for development only.
    # In production, use gunicorn:
    #   gunicorn -w 4 -b 0.0.0.0:8080 "regulatory_reporter.main:create_app()"
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
