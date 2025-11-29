FROM python:3.11.7-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir .

# Copy application code
COPY orchestrator/ orchestrator/

# Copy dbt project (mounted or baked in)
COPY dbt_project/ dbt_project/

# Copy source specs
COPY source_specs/ source_specs/

# Set environment
ENV PYTHONUNBUFFERED=1
ENV DBT_PROJECT_DIR=/app/dbt_project
ENV DBT_PROFILES_DIR=/app/dbt_project
ENV SOURCE_SPECS_DIR=/app/source_specs

# Run orchestrator
ENTRYPOINT ["python", "-m", "orchestrator.orchestrator.main"]
