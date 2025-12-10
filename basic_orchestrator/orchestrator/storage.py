"""
Storage abstraction layer for the Orchestrator.

Provides a unified interface for file operations that works with both
local filesystem (development) and Google Cloud Storage (production).

The Protocol pattern allows the loader to work with any storage backend
without knowing the implementation details.
"""

from typing import Protocol
from pathlib import Path


class Storage(Protocol):
    """
    Protocol defining the storage interface.
    
    Any storage backend must implement these four methods:
    - list_files: Find files in a directory/prefix
    - read_file: Get file contents as bytes
    - move: Move a file from one location to another
    - join: Combine a base path with a filename
    """
    
    def list_files(self, path: str) -> list[str]:
        """List all CSV files in the given path."""
        ...
    
    def read_file(self, path: str) -> bytes:
        """Read and return the entire contents of a file."""
        ...
    
    def move(self, src: str, dst: str) -> None:
        """Move a file from src to dst."""
        ...
    
    def join(self, base: str, filename: str) -> str:
        """Join a base path with a filename."""
        ...


class LocalStorage:
    """
    Local filesystem storage implementation.
    
    Used for local development with DuckDB. Files are accessed directly
    via the filesystem using pathlib.
    """
    
    def list_files(self, path: str) -> list[str]:
        """
        List all files in a directory.
        
        Note: Returns all files, not just CSVs, so the caller can also
        see go files and other control files for validation.
        
        Args:
            path: Directory path to list
        
        Returns:
            List of absolute file paths as strings
        """
        directory = Path(path)
        
        # Create directory if it doesn't exist (convenience for local dev)
        directory.mkdir(parents=True, exist_ok=True)
        
        # Return all files (not directories) in the path
        return [str(f) for f in directory.iterdir() if f.is_file()]
    
    def read_file(self, path: str) -> bytes:
        """
        Read file contents as bytes.
        
        Args:
            path: Path to the file
        
        Returns:
            File contents as bytes
        """
        return Path(path).read_bytes()
    
    def move(self, src: str, dst: str) -> None:
        """
        Move a file from source to destination.
        
        Creates the destination directory if it doesn't exist.
        
        Args:
            src: Source file path
            dst: Destination file path
        """
        dst_path = Path(dst)
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        Path(src).rename(dst_path)
    
    def join(self, base: str, filename: str) -> str:
        """
        Join a base path with a filename.
        
        Args:
            base: Base directory path
            filename: Filename to append
        
        Returns:
            Combined path as string
        """
        return str(Path(base) / filename)


class GCSStorage:
    """
    Google Cloud Storage implementation.
    
    Used in production with BigQuery. GCS provides atomic uploads,
    so files are either fully present or not visible at all - no
    need for file readiness checks.
    """
    
    def __init__(self):
        """
        Initialise the GCS client.
        
        Uses Application Default Credentials, which automatically
        works with GKE Workload Identity in production.
        """
        from google.cloud import storage
        self.client = storage.Client()
    
    def _parse_gcs_path(self, path: str) -> tuple[str, str]:
        """
        Parse a gs:// URI into bucket and prefix.
        
        Args:
            path: GCS path like "gs://bucket/prefix/path"
        
        Returns:
            Tuple of (bucket_name, prefix)
        
        Example:
            "gs://my-bucket/data/landing" -> ("my-bucket", "data/landing")
        """
        # Remove the gs:// prefix
        path = path.removeprefix("gs://")
        # Split into bucket and the rest
        bucket, _, prefix = path.partition("/")
        return bucket, prefix
    
    def list_files(self, path: str) -> list[str]:
        """
        List all files (blobs) under a GCS prefix.
        
        Args:
            path: GCS path like "gs://bucket/prefix"
        
        Returns:
            List of full GCS paths for each blob
        """
        bucket_name, prefix = self._parse_gcs_path(path)
        
        # Ensure prefix ends with / for directory-like listing
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        bucket = self.client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        # Return full gs:// paths
        return [f"gs://{bucket_name}/{blob.name}" for blob in blobs]
    
    def read_file(self, path: str) -> bytes:
        """
        Download and return a blob's contents.
        
        Args:
            path: Full GCS path like "gs://bucket/path/to/file.csv"
        
        Returns:
            File contents as bytes
        """
        bucket_name, key = self._parse_gcs_path(path)
        blob = self.client.bucket(bucket_name).blob(key)
        return blob.download_as_bytes()
    
    def move(self, src: str, dst: str) -> None:
        """
        Move a blob from source to destination.
        
        GCS doesn't have a native move operation, so this copies
        the blob to the new location then deletes the original.
        
        Args:
            src: Source GCS path
            dst: Destination GCS path
        """
        src_bucket_name, src_key = self._parse_gcs_path(src)
        dst_bucket_name, dst_key = self._parse_gcs_path(dst)
        
        src_bucket = self.client.bucket(src_bucket_name)
        dst_bucket = self.client.bucket(dst_bucket_name)
        
        src_blob = src_bucket.blob(src_key)
        
        # Copy to new location
        src_bucket.copy_blob(src_blob, dst_bucket, dst_key)
        
        # Delete original
        src_blob.delete()
    
    def join(self, base: str, filename: str) -> str:
        """
        Join a GCS base path with a filename.
        
        Args:
            base: Base GCS path like "gs://bucket/prefix"
            filename: Filename to append
        
        Returns:
            Combined GCS path
        """
        # Strip trailing slash from base if present, then append
        return f"{base.rstrip('/')}/{filename}"
