# Orchestrator Dockerfile
# Multi-stage build for smaller final image

FROM python:3.12-slim as builder

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

# Final stage
FROM python:3.12-slim

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /app/deps /app/deps
ENV PYTHONPATH=/app/deps:/app

# Copy application code
COPY orchestrator/ /app/orchestrator/

# Create non-root user
RUN useradd --create-home appuser
USER appuser

# Default command
CMD ["python", "-m", "orchestrator.main"]
