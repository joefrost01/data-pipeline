"""Configuration loaded from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Pipeline configuration."""
    
    # Environment
    env: str
    project_id: str
    region: str
    
    # GCS buckets
    landing_bucket: str
    staging_bucket: str
    archive_bucket: str
    failed_bucket: str
    extracts_bucket: str
    
    # BigQuery
    bq_location: str
    control_dataset: str
    
    # dbt
    dbt_project_dir: str
    dbt_profiles_dir: str
    dbt_target: str
    
    # Extract
    extract_hour: int  # UTC hour to generate extract
    extract_format: str  # jsonl or avro
    extract_window_days: int
    
    # Metrics
    dynatrace_endpoint: str
    dynatrace_token_path: str
    
    # Source specs
    source_specs_dir: str
    
    @classmethod
    def from_env(cls) -> "Config":
        """Load configuration from environment variables."""
        env = os.environ.get("ENV", "int")
        project_id = os.environ["PROJECT_ID"]
        
        return cls(
            env=env,
            project_id=project_id,
            region=os.environ.get("REGION", "europe-west2"),
            
            landing_bucket=os.environ.get("LANDING_BUCKET", f"surveillance-{env}-landing"),
            staging_bucket=os.environ.get("STAGING_BUCKET", f"surveillance-{env}-staging"),
            archive_bucket=os.environ.get("ARCHIVE_BUCKET", f"surveillance-{env}-archive"),
            failed_bucket=os.environ.get("FAILED_BUCKET", f"surveillance-{env}-failed"),
            extracts_bucket=os.environ.get("EXTRACTS_BUCKET", f"surveillance-{env}-extracts"),
            
            bq_location=os.environ.get("BQ_LOCATION", "europe-west2"),
            control_dataset=os.environ.get("CONTROL_DATASET", "control"),
            
            dbt_project_dir=os.environ.get("DBT_PROJECT_DIR", "/app/dbt_project"),
            dbt_profiles_dir=os.environ.get("DBT_PROFILES_DIR", "/app/dbt_project"),
            dbt_target=os.environ.get("DBT_TARGET", env),
            
            extract_hour=int(os.environ.get("EXTRACT_HOUR", "6")),
            extract_format=os.environ.get("EXTRACT_FORMAT", "jsonl"),
            extract_window_days=int(os.environ.get("EXTRACT_WINDOW_DAYS", "7")),
            
            dynatrace_endpoint=os.environ.get("DYNATRACE_ENDPOINT", ""),
            dynatrace_token_path=os.environ.get("DYNATRACE_TOKEN_PATH", "/secrets/dynatrace-token"),
            
            source_specs_dir=os.environ.get("SOURCE_SPECS_DIR", "/app/source_specs"),
        )
