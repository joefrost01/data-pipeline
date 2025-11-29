"""Regulatory Reporter Service.

Low-latency service for submitting regulatory reports within 15-minute SLA.
Features:
- In-memory cache with periodic refresh
- Cache-aside pattern for cache misses
- Admin endpoint for manual cache refresh
- Idempotent submissions
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
class ReferenceCache:
    """In-memory cache for reference data."""
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
                # Only add to name lookup if name doesn't already exist
                # (first one wins, prevents silent overwrites)
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
    
    def _lookup_with_fallback(
        self, cache_dict: dict, key: str, table: str, key_column: str
    ) -> dict | None:
        """Look up reference data with cache-aside pattern.
        
        First checks cache, then falls back to BigQuery on miss.
        """
        # Check cache first
        if key in cache_dict:
            return cache_dict[key]
        
        # Cache miss - query BigQuery directly
        log.warning("cache_miss", table=table, key=key)
        
        try:
            query = f"""
                SELECT * FROM {table}
                WHERE {key_column} = @key
                LIMIT 1
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("key", "STRING", key)
                ]
            )
            
            results = list(self.bq_client.query(
                query,
                job_config=job_config,
                timeout=self.cache_config.cache_miss_timeout_seconds,
            ).result())
            
            if results:
                data = dict(results[0])
                # Update cache with the found value
                cache_dict[key] = data
                return data
            
            return None
            
        except Exception as e:
            log.error("cache_fallback_failed", table=table, key=key, error=str(e))
            return None
    
    def get_trader(self, trader_id: str) -> dict | None:
        """Get trader reference data with cache-aside fallback."""
        return self._lookup_with_fallback(
            self.cache.traders,
            trader_id,
            "snapshots.traders_snapshot",
            "trader_id",
        )
    
    def get_counterparty(self, counterparty_id: str) -> dict | None:
        """Get counterparty reference data by ID with cache-aside fallback."""
        return self._lookup_with_fallback(
            self.cache.counterparties_by_id,
            counterparty_id,
            "snapshots.counterparties_snapshot",
            "counterparty_id",
        )
    
    def get_counterparty_by_name(self, counterparty_name: str) -> dict | None:
        """Get counterparty reference data by name with cache-aside fallback."""
        return self._lookup_with_fallback(
            self.cache.counterparties_by_name,
            counterparty_name,
            "snapshots.counterparties_snapshot",
            "counterparty_name",
        )
    
    def get_instrument(self, instrument_id: str) -> dict | None:
        """Get instrument reference data with cache-aside fallback."""
        return self._lookup_with_fallback(
            self.cache.instruments,
            instrument_id,
            "curation.dim_instrument",
            "instrument_id",
        )
    
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
        
        # Submit to regulator
        submission_start = time.monotonic()
        
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
            response.raise_for_status()
            
            result = response.json()
            regulator_reference = result.get("reference", result.get("id"))
            
            submission_latency = time.monotonic() - submission_start
            total_latency = (
                datetime.now(timezone.utc) - event_timestamp
            ).total_seconds()
            
            # Log to audit table
            self._log_submission(
                event_id=event_id,
                event_timestamp=event_timestamp,
                regulator_reference=regulator_reference,
                submission_latency=submission_latency,
                total_latency=total_latency,
                status="SUBMITTED",
                payload_hash=self._hash_payload(enriched),
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
            
        except httpx.HTTPStatusError as e:
            log.error(
                "submission_http_error",
                event_id=event_id,
                status_code=e.response.status_code,
                body=e.response.text[:500],
            )
            return {
                "status": "error",
                "event_id": event_id,
                "message": f"HTTP {e.response.status_code}: {e.response.text[:200]}",
            }
        except Exception as e:
            log.exception("submission_failed", event_id=event_id)
            return {
                "status": "error",
                "event_id": event_id,
                "message": str(e),
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
            "report_type": "TRADE",  # Could be parameterised
            "report_payload_hash": payload_hash,
            "retry_count": 0,
        }
        
        errors = self.bq_client.insert_rows_json(
            "control.regulatory_submissions", [row]
        )
        
        if errors:
            log.error("audit_log_failed", errors=errors)
    
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
                    "counterparties": len(self.cache.counterparties_by_id),
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


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
    )
    
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
