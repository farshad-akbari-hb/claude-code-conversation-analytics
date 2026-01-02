# Claude Analytics Platform Dockerfile
# Multi-stage build for efficient production image

# =============================================================================
# Stage 1: Builder - Install dependencies
# =============================================================================
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Install dbt dependencies
RUN pip install --no-cache-dir dbt-core dbt-duckdb

# =============================================================================
# Stage 2: Runtime - Final production image
# =============================================================================
FROM python:3.11-slim as runtime

WORKDIR /app

# Install runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# dbt environment
ENV DBT_PROJECT_DIR=/app/dbt \
    DBT_PROFILES_DIR=/app/dbt \
    DBT_TARGET=prod

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash analytics && \
    chown -R analytics:analytics /app

# Create data directories
RUN mkdir -p /data/raw /data/incremental /data/dead_letter /duckdb && \
    chown -R analytics:analytics /data /duckdb

# Copy application code
COPY --chown=analytics:analytics . .

# Create dbt logs directory with correct ownership before dbt deps
RUN mkdir -p /app/dbt/logs /app/dbt/target && \
    chown -R analytics:analytics /app/dbt

# Switch to non-root user before running dbt
USER analytics

# Install dbt packages (now runs as analytics user)
RUN cd /app/dbt && dbt deps --profiles-dir /app/dbt

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command - can be overridden in docker-compose
CMD ["python", "-m", "analytics.cli", "--help"]
