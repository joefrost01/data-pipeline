"""Generate surveillance extract for partner."""

from dataclasses import dataclass
from datetime import datetime, timezone

import structlog
from google.cloud import bigquery, storage

from orchestrator.config import Config
from orchestrator.metrics import MetricsClient

log = structlog.get_logger()


@dataclass
class ExtractResult:
    """Result of extract generation."""
    output_path: str
    row_count: int
    size_bytes: int


class ExtractGenerator:
    """Generates 7-day rolling window extract for surveillance partner."""
    
    def __init__(self, config: Config, metrics: MetricsClient) -> None:
        self.config = config
        self.metrics = metrics
        self.bq_client = bigquery.Client(location=config.bq_location)
        self.storage_client = storage.Client()
    
    def run(self) -> ExtractResult:
        """Generate extract and write to GCS."""
        today = datetime.now(timezone.utc).date()
        
        # Determine output format and path
        if self.config.extract_format == "avro":
            extension = "avro"
            destination_format = bigquery.DestinationFormat.AVRO
        else:
            extension = "jsonl.gz"
            destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        
        output_path = (
            f"gs://{self.config.extracts_bucket}/"
            f"{today.isoformat()}/"
            f"surveillance_extract_{today.isoformat()}.{extension}"
        )
        
        # Query for row count first (for logging)
        count_query = f"""
            SELECT COUNT(*) as cnt
            FROM consumer.surveillance_extract
            WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.config.extract_window_days} DAY)
        """
        count_result = self.bq_client.query(count_query).result()
        row_count = list(count_result)[0].cnt
        
        log.info(
            "extract_starting",
            row_count=row_count,
            window_days=self.config.extract_window_days,
            output_path=output_path,
        )
        
        # Extract to GCS
        extract_query = f"""
            SELECT *
            FROM consumer.surveillance_extract
            WHERE trade_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {self.config.extract_window_days} DAY)
        """
        
        job_config = bigquery.ExtractJobConfig(
            destination_format=destination_format,
            compression=bigquery.Compression.GZIP if extension.endswith(".gz") else None,
        )
        
        # Create temp table from query, then extract
        # (bq extract only works on tables, not queries)
        temp_table = f"{self.config.project_id}.control._extract_temp_{today.isoformat().replace('-', '')}"
        
        query_job = self.bq_client.query(
            extract_query,
            job_config=bigquery.QueryJobConfig(destination=temp_table),
        )
        query_job.result()  # Wait for query
        
        # Extract temp table to GCS
        extract_job = self.bq_client.extract_table(
            temp_table,
            output_path,
            job_config=job_config,
        )
        extract_job.result()  # Wait for extract
        
        # Clean up temp table
        self.bq_client.delete_table(temp_table, not_found_ok=True)
        
        # Get file size
        bucket_name = self.config.extracts_bucket
        blob_path = output_path.replace(f"gs://{bucket_name}/", "")
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.reload()
        size_bytes = blob.size or 0
        
        log.info(
            "extract_complete",
            output_path=output_path,
            row_count=row_count,
            size_bytes=size_bytes,
        )
        
        return ExtractResult(
            output_path=output_path,
            row_count=row_count,
            size_bytes=size_bytes,
        )
