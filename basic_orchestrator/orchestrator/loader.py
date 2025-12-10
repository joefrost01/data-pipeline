"""
Data loader implementations for the Orchestrator.

Provides a unified interface for loading Polars DataFrames into either
DuckDB (local development) or BigQuery (production).

Both implementations:
- Create a load_metadata table for tracking all loads
- Support schema introspection for drift handling
- Record metadata for every load operation
"""

from typing import Protocol
from dataclasses import dataclass
from datetime import datetime
import polars as pl


@dataclass
class LoadResult:
    """
    Metadata captured for each file load operation.
    
    This data is stored in the load_metadata table and can be joined
    with raw tables via the _load_id column for full traceability.
    """
    load_id: str            # UUID identifying this specific load
    filename: str           # Original source filename
    table: str              # Target table name
    row_count: int          # Number of rows loaded
    started_at: datetime    # When the load began
    completed_at: datetime  # When the load completed


class DataLoader(Protocol):
    """
    Protocol defining the data loader interface.
    
    Any loader backend must implement these three methods:
    - load: Insert a DataFrame into a table
    - record_metadata: Record load metadata
    - get_columns: Get existing column names for a table (or None if new)
    """
    
    def load(self, df: pl.DataFrame, table: str) -> None:
        """Load a DataFrame into the specified table."""
        ...
    
    def record_metadata(self, result: LoadResult) -> None:
        """Record load metadata to the load_metadata table."""
        ...
    
    def get_columns(self, table: str) -> list[str] | None:
        """
        Get existing column names for a table.
        
        Returns None if the table doesn't exist (first load).
        Excludes internal columns (those starting with underscore).
        """
        ...


class DuckDBLoader:
    """
    DuckDB data loader implementation.
    
    Used for local development. Leverages DuckDB's ability to query
    Polars DataFrames directly via Apache Arrow for zero-copy loading.
    """
    
    def __init__(self, db_path: str):
        """
        Initialise the DuckDB connection.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        import duckdb
        self.conn = duckdb.connect(db_path)
        self._ensure_metadata_table()
    
    def _ensure_metadata_table(self) -> None:
        """Create the load_metadata table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS load_metadata (
                load_id VARCHAR PRIMARY KEY,
                filename VARCHAR NOT NULL,
                table_name VARCHAR NOT NULL,
                row_count INTEGER NOT NULL,
                started_at TIMESTAMP NOT NULL,
                completed_at TIMESTAMP NOT NULL
            )
        """)
    
    def get_columns(self, table: str) -> list[str] | None:
        """
        Get column names for an existing table.
        
        Args:
            table: Table name to inspect
        
        Returns:
            List of column names (excluding _ prefixed internal columns),
            or None if the table doesn't exist.
        """
        try:
            result = self.conn.execute(f"DESCRIBE {table}").fetchall()
            # Filter out internal columns (those starting with _)
            return [row[0] for row in result if not row[0].startswith("_")]
        except Exception:
            # Table doesn't exist
            return None
    
    def load(self, df: pl.DataFrame, table: str) -> None:
        """
        Load a Polars DataFrame into a DuckDB table.
        
        DuckDB can query Polars DataFrames directly through Arrow,
        making this very efficient - essentially zero-copy.
        
        If the table doesn't exist, it's created with the DataFrame's schema.
        If it exists, data is appended.
        
        Args:
            df: Polars DataFrame to load
            table: Target table name
        """
        # Create table if it doesn't exist (using empty result to get schema)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df WHERE 1=0"
        )
        
        # Insert data - DuckDB can reference 'df' directly from the Python scope
        self.conn.execute(f"INSERT INTO {table} SELECT * FROM df")
    
    def record_metadata(self, result: LoadResult) -> None:
        """
        Record load metadata to the tracking table.
        
        Args:
            result: LoadResult containing load details
        """
        self.conn.execute(
            """
            INSERT INTO load_metadata 
                (load_id, filename, table_name, row_count, started_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                result.load_id,
                result.filename,
                result.table,
                result.row_count,
                result.started_at,
                result.completed_at,
            ]
        )


class BigQueryLoader:
    """
    BigQuery data loader implementation.
    
    Used in production. Loads data via GCS staging for efficiency:
    1. Write DataFrame to Parquet in GCS staging bucket
    2. Load from GCS to BigQuery (BigQuery's preferred bulk load path)
    3. Clean up staging file
    
    This approach handles large volumes much better than streaming inserts.
    """
    
    def __init__(self, project: str, dataset: str, staging_bucket: str):
        """
        Initialise the BigQuery loader.
        
        Args:
            project: GCP project ID
            dataset: BigQuery dataset name
            staging_bucket: GCS bucket name for Parquet staging
        """
        from google.cloud import bigquery, storage
        
        self.bq_client = bigquery.Client(project=project)
        self.gcs_client = storage.Client()
        self.dataset = f"{project}.{dataset}"
        self.staging_bucket = staging_bucket
        
        self._ensure_metadata_table()
    
    def _ensure_metadata_table(self) -> None:
        """Create the load_metadata table if it doesn't exist."""
        from google.cloud import bigquery
        
        schema = [
            bigquery.SchemaField("load_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("row_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("completed_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table_ref = f"{self.dataset}.load_metadata"
        table = bigquery.Table(table_ref, schema=schema)
        self.bq_client.create_table(table, exists_ok=True)
    
    def get_columns(self, table: str) -> list[str] | None:
        """
        Get column names for an existing BigQuery table.
        
        Args:
            table: Table name to inspect
        
        Returns:
            List of column names (excluding _ prefixed internal columns),
            or None if the table doesn't exist.
        """
        from google.cloud.exceptions import NotFound
        
        try:
            table_ref = f"{self.dataset}.{table}"
            bq_table = self.bq_client.get_table(table_ref)
            # Filter out internal columns (those starting with _)
            return [field.name for field in bq_table.schema if not field.name.startswith("_")]
        except NotFound:
            return None
    
    def load(self, df: pl.DataFrame, table: str) -> None:
        """
        Load a Polars DataFrame into BigQuery via GCS staging.
        
        Process:
        1. Write DataFrame to Parquet in GCS staging location
        2. Create BigQuery load job from GCS
        3. Wait for job completion
        4. Delete staging file
        
        This is much faster than streaming inserts for bulk data.
        
        Args:
            df: Polars DataFrame to load
            table: Target table name
        """
        from google.cloud import bigquery
        import uuid
        import io
        
        # Generate unique staging filename
        staging_filename = f"staging/{table}_{uuid.uuid4().hex}.parquet"
        
        # Write Parquet to GCS
        bucket = self.gcs_client.bucket(self.staging_bucket)
        blob = bucket.blob(staging_filename)
        
        # Polars write_parquet returns bytes when given no path
        parquet_buffer = io.BytesIO()
        df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        blob.upload_from_file(parquet_buffer)
        
        try:
            # Configure and run BigQuery load job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # Parquet is self-describing, so schema comes from the file
                autodetect=True,
            )
            
            uri = f"gs://{self.staging_bucket}/{staging_filename}"
            table_ref = f"{self.dataset}.{table}"
            
            load_job = self.bq_client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config,
            )
            
            # Wait for job to complete (raises on error)
            load_job.result()
            
        finally:
            # Always clean up staging file
            blob.delete()
    
    def record_metadata(self, result: LoadResult) -> None:
        """
        Record load metadata to the tracking table.
        
        Uses streaming insert for single-row metadata (efficient for small writes).
        
        Args:
            result: LoadResult containing load details
        """
        rows = [{
            "load_id": result.load_id,
            "filename": result.filename,
            "table_name": result.table,
            "row_count": result.row_count,
            "started_at": result.started_at.isoformat(),
            "completed_at": result.completed_at.isoformat(),
        }]
        
        table_ref = f"{self.dataset}.load_metadata"
        errors = self.bq_client.insert_rows_json(table_ref, rows)
        
        if errors:
            raise RuntimeError(f"Failed to insert metadata: {errors}")
