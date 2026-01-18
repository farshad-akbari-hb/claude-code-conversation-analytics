# Claude Analytics Platform - Makefile
# Convenience commands for managing the analytics pipeline

.PHONY: up up-prefect down rebuild fresh deploy worker-local logs status \
        run-adhoc run-backfill run-daily pipeline shell help configure-metabase \
        pause-metabase resume-metabase safe-pipeline safe-backfill safe-adhoc \
        export-db import-db

# Default target
help:
	@echo "Claude Analytics Platform - Available Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up           - Start all services (Prefect, Worker, Metabase)"
	@echo "  make up-prefect   - Start only Prefect infrastructure"
	@echo "  make down         - Stop all services"
	@echo "  make rebuild      - Rebuild all containers (no cache)"
	@echo "  make fresh        - Full reset: down + rebuild + up + deploy"
	@echo "  make logs         - View worker logs (Ctrl+C to exit)"
	@echo "  make shell        - Open shell in analytics-worker container"
	@echo ""
	@echo "Deployments:"
	@echo "  make deploy       - Deploy flows to Prefect server"
	@echo "  make status       - Show deployment status"
	@echo ""
	@echo "Run Pipeline (with DuckDB lock handling):"
	@echo "  make safe-backfill - Full backfill (pauses Metabase, runs, resumes)"
	@echo "  make safe-adhoc    - Incremental run (pauses Metabase, runs, resumes)"
	@echo "  make safe-pipeline - Direct pipeline (pauses Metabase, runs, resumes)"
	@echo ""
	@echo "Run Pipeline (manual - requires Metabase stopped):"
	@echo "  make run-adhoc    - Trigger incremental pipeline run"
	@echo "  make run-backfill - Trigger full historical backfill"
	@echo "  make run-daily    - Trigger daily full refresh"
	@echo "  make pipeline     - Run pipeline directly (no Prefect)"
	@echo ""
	@echo "Metabase Control:"
	@echo "  make pause-metabase  - Stop Metabase to release DuckDB lock"
	@echo "  make resume-metabase - Restart Metabase after pipeline"
	@echo ""
	@echo "Data Management:"
	@echo "  make export-db       - Export DuckDB to ./analytics.db"
	@echo "  make import-db       - Import DuckDB from ./analytics.db"
	@echo ""
	@echo "Development:"
	@echo "  make worker-local - Start local Prefect worker"
	@echo ""
	@echo "Configuration:"
	@echo "  make configure-metabase - Set Metabase DuckDB to read-only mode"
	@echo ""

# =============================================================================
# Infrastructure
# =============================================================================

# Start all services (server + worker + metabase)
up:
	docker-compose -f docker-compose.analytics.yml up -d
	@echo ""
	@echo "Services started:"
	@echo "  - Prefect UI: http://localhost:4200"
	@echo "  - Metabase:   http://localhost:3001"
	@echo "  - dbt docs:   http://localhost:8686"

# Start only Prefect infrastructure (no Metabase)
up-prefect:
	docker-compose -f docker-compose.analytics.yml up -d \
		prefect-server prefect-db prefect-worker analytics-worker
	@echo ""
	@echo "Prefect infrastructure started:"
	@echo "  - Prefect UI: http://localhost:4200"
	@echo ""
	@echo "Next: Run 'make deploy' to register flows"

# Stop all services
down:
	docker-compose -f docker-compose.analytics.yml down

# Rebuild all containers with no cache (ensures matching versions)
rebuild:
	@echo "Rebuilding all containers (no cache)..."
	docker-compose -f docker-compose.analytics.yml build --no-cache
	@echo ""
	@echo "Rebuild complete. Run 'make up' to start services."

# Fresh start: stop, rebuild, start, and deploy (clean slate)
fresh: down rebuild up
	@echo ""
	@echo "Waiting for services to be healthy (20s)..."
	@sleep 20
	@$(MAKE) deploy

# View worker logs
logs:
	docker-compose -f docker-compose.analytics.yml logs -f prefect-worker

# Open shell in analytics-worker container
shell:
	docker-compose -f docker-compose.analytics.yml exec analytics-worker bash

# =============================================================================
# Deployments
# =============================================================================

# Deploy flows to Prefect server (rebuilds to ensure latest code)
deploy:
	@echo "Building latest image..."
	docker-compose -f docker-compose.analytics.yml build analytics-worker
	@echo "Restarting worker with new image..."
	docker-compose -f docker-compose.analytics.yml up -d analytics-worker
	@sleep 5
	@echo "Deploying flows to Prefect server..."
	docker-compose -f docker-compose.analytics.yml exec \
		-w /app analytics-worker prefect --no-prompt deploy --all
	@echo ""
	@echo "Deployments registered. View at: http://localhost:4200/deployments"

# Show deployment status
status:
	@docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment ls

# =============================================================================
# Metabase Control (for DuckDB lock management)
# =============================================================================

# Stop Metabase to release DuckDB write lock
pause-metabase:
	@echo "Pausing Metabase to release DuckDB lock..."
	@docker-compose -f docker-compose.analytics.yml stop metabase dbt-docs 2>/dev/null || true
	@echo "Metabase paused. DuckDB is now available for writes."

# Restart Metabase after pipeline completes
resume-metabase:
	@echo "Resuming Metabase..."
	@docker-compose -f docker-compose.analytics.yml start metabase dbt-docs 2>/dev/null || true
	@echo "Metabase resumed. Dashboard available at http://localhost:3001"

# =============================================================================
# Safe Pipeline Runs (with automatic Metabase coordination)
# These targets pause Metabase, run the pipeline, and resume Metabase
# =============================================================================

# Full backfill with Metabase coordination
safe-backfill:
	@echo "=============================================="
	@echo "Safe Backfill: Coordinating with Metabase"
	@echo "=============================================="
	@$(MAKE) pause-metabase
	@echo ""
	@echo "Running full backfill..."
	@docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/full-backfill' || \
		(echo "Pipeline failed, resuming Metabase anyway..." && $(MAKE) resume-metabase && exit 1)
	@echo ""
	@echo "Waiting for pipeline to complete..."
	@echo "Monitor progress at: http://localhost:4200"
	@echo ""
	@echo "When complete, run: make resume-metabase"

# Incremental run with Metabase coordination
safe-adhoc:
	@echo "=============================================="
	@echo "Safe Ad-hoc Run: Coordinating with Metabase"
	@echo "=============================================="
	@$(MAKE) pause-metabase
	@echo ""
	@echo "Running incremental pipeline..."
	@docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/adhoc-analytics' || \
		(echo "Pipeline failed, resuming Metabase anyway..." && $(MAKE) resume-metabase && exit 1)
	@echo ""
	@echo "Waiting for pipeline to complete..."
	@echo "Monitor progress at: http://localhost:4200"
	@echo ""
	@echo "When complete, run: make resume-metabase"

# Direct pipeline run with Metabase coordination (blocking)
safe-pipeline:
	@echo "=============================================="
	@echo "Safe Pipeline: Coordinating with Metabase"
	@echo "=============================================="
	@$(MAKE) pause-metabase
	@echo ""
	@echo "Running pipeline (this may take a while)..."
	@docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker claude-analytics pipeline --verbose; \
		RESULT=$$?; \
		$(MAKE) resume-metabase; \
		exit $$RESULT
	@echo ""
	@echo "Pipeline complete. Metabase resumed."

# =============================================================================
# Run Pipeline (Manual - requires Metabase to be stopped first)
# =============================================================================

# Trigger ad-hoc incremental pipeline run
run-adhoc:
	@echo "Triggering ad-hoc pipeline run..."
	@echo "NOTE: If you get lock errors, run 'make safe-adhoc' instead"
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/adhoc-analytics'

# Trigger full backfill (initial load or recovery)
run-backfill:
	@echo "Triggering full backfill..."
	@echo "NOTE: If you get lock errors, run 'make safe-backfill' instead"
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/full-backfill'

# Trigger daily full refresh
run-daily:
	@echo "Triggering daily full refresh..."
	@echo "NOTE: If you get lock errors, run 'make pause-metabase' first"
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/daily-full-refresh'

# Run pipeline directly without Prefect (useful for debugging)
pipeline:
	@echo "NOTE: If you get lock errors, run 'make safe-pipeline' instead"
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker claude-analytics pipeline --verbose

# =============================================================================
# Development
# =============================================================================

# Start local Prefect worker (for development/debugging)
worker-local:
	@echo "Starting local worker..."
	@echo "Make sure PREFECT_API_URL is set to http://localhost:4200/api"
	PREFECT_API_URL=http://localhost:4200/api prefect worker start --pool analytics-pool

# =============================================================================
# Data Management
# =============================================================================

# Export DuckDB database to local filesystem (for inspection/backup)
export-db:
	@echo "Exporting DuckDB database to ./analytics.db..."
	@docker cp analytics-worker:/duckdb/analytics.db ./analytics.db 2>/dev/null || \
		docker cp prefect-worker:/duckdb/analytics.db ./analytics.db
	@echo "Exported to ./analytics.db ($(shell ls -lh ./analytics.db 2>/dev/null | awk '{print $$5}'))"
	@echo ""
	@echo "You can now open with: duckdb ./analytics.db"

# Import DuckDB database from local filesystem (restore from backup)
import-db:
	@echo "Importing DuckDB database from ./analytics.db..."
	@test -f ./analytics.db || (echo "Error: ./analytics.db not found" && exit 1)
	@$(MAKE) pause-metabase
	@docker cp ./analytics.db analytics-worker:/duckdb/analytics.db 2>/dev/null || \
		docker cp ./analytics.db prefect-worker:/duckdb/analytics.db
	@$(MAKE) resume-metabase
	@echo "Imported successfully"

# =============================================================================
# Configuration
# =============================================================================

# Configure Metabase DuckDB connection for read-only mode
# This prevents lock conflicts between Metabase and the ETL pipeline
configure-metabase:
	@echo "Configuring Metabase DuckDB connection for read-only mode..."
	@echo "Note: Requires METABASE_PASSWORD environment variable"
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker python /app/scripts/configure_metabase_readonly.py
