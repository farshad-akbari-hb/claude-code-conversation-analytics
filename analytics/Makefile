# Claude Analytics Platform - Makefile
# Convenience commands for managing the analytics pipeline

.PHONY: up up-prefect down deploy worker-local logs status \
        run-adhoc run-backfill run-daily pipeline shell help

# Default target
help:
	@echo "Claude Analytics Platform - Available Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up           - Start all services (Prefect, Worker, Metabase)"
	@echo "  make up-prefect   - Start only Prefect infrastructure"
	@echo "  make down         - Stop all services"
	@echo "  make logs         - View worker logs (Ctrl+C to exit)"
	@echo "  make shell        - Open shell in analytics-worker container"
	@echo ""
	@echo "Deployments:"
	@echo "  make deploy       - Deploy flows to Prefect server"
	@echo "  make status       - Show deployment status"
	@echo ""
	@echo "Run Pipeline:"
	@echo "  make run-adhoc    - Trigger incremental pipeline run"
	@echo "  make run-backfill - Trigger full historical backfill"
	@echo "  make run-daily    - Trigger daily full refresh"
	@echo "  make pipeline     - Run pipeline directly (no Prefect)"
	@echo ""
	@echo "Development:"
	@echo "  make worker-local - Start local Prefect worker"
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
	@echo "  - dbt docs:   http://localhost:8080"

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
# Run Pipeline
# =============================================================================

# Trigger ad-hoc incremental pipeline run
run-adhoc:
	@echo "Triggering ad-hoc pipeline run..."
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/adhoc-analytics'

# Trigger full backfill (initial load or recovery)
run-backfill:
	@echo "Triggering full backfill..."
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/full-backfill'

# Trigger daily full refresh
run-daily:
	@echo "Triggering daily full refresh..."
	docker-compose -f docker-compose.analytics.yml exec \
		analytics-worker prefect deployment run 'claude-analytics-pipeline/daily-full-refresh'

# Run pipeline directly without Prefect (useful for debugging)
pipeline:
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
