# Implementation Plan: Claude Conversation Analytics Platform

## Overview

Implement an ELT analytics platform for Claude Code conversation logs as specified in `docs/SPEC.md`. The platform extracts data from the existing MongoDB `conversations` collection, loads it into DuckDB via Parquet intermediate files, transforms it using dbt (medallion architecture), and visualizes insights through Metabase.

## Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Scope** | Full Implementation | All 14 phases (CDC deferred) |
| **Real-time CDC** | Skip initially | Start with batch extraction (hourly), add CDC later |
| **Historical Backfill** | Yes, full backfill | Extract all existing MongoDB data on first run |

## Current State

- **Sync Service** (`src/`): Already syncs JSONL → SQLite → MongoDB
- **MongoDB Collection**: `claude_logs.conversations` with documents containing:
  - `_id`, `type`, `sessionId`, `timestamp`, `message`, `projectId`, `sourceFile`, `ingestedAt`
- **UI** (`ui/`): Next.js app with basic filtering, search, and stats

## Target Architecture (Initial Release)

```
MongoDB (Source) → Python Extractor → Parquet Files → DuckDB → dbt → Metabase
                        ↑
                  Prefect Orchestrator (Batch: Hourly)
```

*CDC Listener will be added in a future enhancement phase*

---

## Implementation Phases

### Phase 1: Project Foundation ✅ COMPLETED
**Goal**: Set up the analytics service structure and dependencies

**Tasks**:
- [x] 1.1 Create `analytics/` directory structure
- [x] 1.2 Create Python project configuration (`pyproject.toml`, `requirements.txt`)
- [x] 1.3 Create base Dockerfile for analytics service
- [x] 1.4 Create `.env.analytics` configuration template
- [x] 1.5 Create `analytics/analytics/__init__.py` package

**Directory Structure**:
```
analytics/
├── Dockerfile
├── pyproject.toml
├── requirements.txt
├── .env.analytics.example
├── analytics/
│   ├── __init__.py
│   └── config.py
├── dbt/
├── great_expectations/
└── data/
    ├── raw/
    ├── incremental/
    └── dead_letter/
```

---

### Phase 2: MongoDB Extractor ✅ COMPLETED
**Goal**: Build Python extractor to read from MongoDB and write Parquet files

**Tasks**:
- [x] 2.1 Create `analytics/analytics/extractor.py` with `MongoExtractor` class
  - [x] 2.1.1 Implement `full_extract()` method for historical backfill
  - [x] 2.1.2 Implement `incremental_extract(since: datetime)` method
  - [x] 2.1.3 Implement `write_parquet(documents, partition_key)` method
  - [x] 2.1.4 Implement high-water mark tracking (last sync timestamp)
- [x] 2.2 Create document transformation logic to flatten nested `message` field
- [x] 2.3 Create Parquet schema definition
- [x] 2.4 Add date-based partitioning (e.g., `date=2025-01-02/`)
- [x] 2.5 Create CLI entry point for manual extraction (`python -m analytics.extractor`)
- [x] 2.6 Add `--full-backfill` flag for initial historical load
- [x] 2.7 Write unit tests for extractor

**Key Dependencies**: `pymongo`, `pyarrow`, `pandas`

---

### Phase 3: DuckDB Loader ✅ COMPLETED
**Goal**: Load Parquet files into DuckDB database

**Tasks**:
- [x] 3.1 Create `analytics/analytics/loader.py` with `DuckDBLoader` class
  - [x] 3.1.1 Implement `create_database()` - initialize DuckDB with schemas
  - [x] 3.1.2 Implement `load_from_parquet(path)` - bulk load from files
  - [x] 3.1.3 Implement `upsert_incremental(path)` - merge new/updated data
- [x] 3.2 Create raw schema (`raw.conversations`)
- [x] 3.3 Add indexes for common query patterns
- [x] 3.4 Create CLI entry point for manual loading
- [x] 3.5 Write unit tests for loader

**Key Dependencies**: `duckdb`, `pyarrow`

---

### Phase 4: dbt Project Setup ✅ COMPLETED
**Goal**: Configure dbt project for Bronze → Silver → Gold transformations

**Tasks**:
- [x] 4.1 Initialize dbt project (`dbt init`)
- [x] 4.2 Configure `dbt_project.yml` for DuckDB
- [x] 4.3 Create `profiles.yml` with dev/prod targets
- [x] 4.4 Install dbt packages (`dbt_utils`, `dbt_date`)
- [x] 4.5 Create source definitions (`sources.yml`)
- [x] 4.6 Create seed file `tool_categories.csv`

---

### Phase 5: dbt Staging Models (Bronze) ✅ COMPLETED
**Goal**: Create staging models that clean and normalize raw data

**Tasks**:
- [x] 5.1 Create `models/staging/stg_conversations.sql`
  - Basic cleaning, type casting, null handling
- [x] 5.2 Create `models/staging/stg_messages.sql`
  - Extract message entries, parse role/content
- [x] 5.3 Create `models/staging/stg_tool_calls.sql`
  - Extract tool_use entries, parse tool name/parameters
- [x] 5.4 Create `models/staging/schema.yml` with tests
- [x] 5.5 Run `dbt test` to validate staging layer

---

### Phase 6: dbt Intermediate Models (Silver) ✅ COMPLETED
**Goal**: Create enriched and joined models

**Tasks**:
- [x] 6.1 Create `models/intermediate/int_messages_enriched.sql`
  - Add task category classification (bug_fix, feature, etc.)
  - Extract hour_of_day, day_of_week
- [x] 6.2 Create `models/intermediate/int_sessions_computed.sql`
  - Compute session start/end times, duration
  - Count messages per session
- [x] 6.3 Create `models/intermediate/int_tool_usage.sql`
  - Parse tool call details, extract file paths
- [x] 6.4 Create `models/intermediate/schema.yml` with tests

---

### Phase 7: dbt Mart Models (Gold) - Dimensions
**Goal**: Create dimension tables for star schema

**Tasks**:
- [ ] 7.1 Create `models/marts/dim_date.sql`
  - Date dimension with day/week/month/quarter attributes
- [ ] 7.2 Create `models/marts/dim_projects.sql`
  - Project dimension with first_seen, last_active
- [ ] 7.3 Create `models/marts/dim_sessions.sql`
  - Session dimension with duration, message counts
- [ ] 7.4 Create `models/marts/dim_tools.sql`
  - Tool dimension with categories
- [ ] 7.5 Create `models/marts/schema.yml` with relationships

---

### Phase 8: dbt Mart Models (Gold) - Facts
**Goal**: Create fact tables for star schema

**Tasks**:
- [ ] 8.1 Create `models/marts/fct_messages.sql`
  - Fact table at message grain with FK to dimensions
- [ ] 8.2 Create `models/marts/fct_tool_calls.sql`
  - Fact table at tool call grain
- [ ] 8.3 Create `models/marts/fct_file_operations.sql`
  - Fact table for file operations (Read/Write/Edit)
- [ ] 8.4 Add incremental strategy configurations
- [ ] 8.5 Create fact table tests

---

### Phase 9: dbt Aggregate Models
**Goal**: Create pre-aggregated tables for dashboards

**Tasks**:
- [ ] 9.1 Create `models/marts/aggregates/agg_session_metrics.sql`
- [ ] 9.2 Create `models/marts/aggregates/agg_tool_efficiency.sql`
- [ ] 9.3 Create `models/marts/aggregates/agg_code_changes.sql`
- [ ] 9.4 Create `models/marts/aggregates/agg_daily_summary.sql`
- [ ] 9.5 Create `models/marts/aggregates/agg_hourly_activity.sql`

---

### Phase 10: Prefect Orchestration
**Goal**: Create pipeline orchestration with Prefect

**Tasks**:
- [ ] 10.1 Create `analytics/analytics/flows/__init__.py`
- [ ] 10.2 Create `analytics/analytics/flows/main_pipeline.py`
  - `@flow analytics_pipeline()` with extract → load → transform tasks
- [ ] 10.3 Create task definitions with retry logic
- [ ] 10.4 Add schedule configuration (hourly batch)
- [ ] 10.5 Create deployment configuration

---

### Phase 11: CDC Listener (DEFERRED - Future Enhancement)
**Goal**: Add MongoDB Change Stream listener for near real-time updates
**Status**: ⏸️ Deferred to post-initial release

*This phase is skipped for initial implementation. Batch extraction (hourly via Prefect) provides sufficient freshness.*

**Future Tasks** (not in current scope):
- [ ] 11.1 Create `analytics/analytics/cdc_listener.py`
- [ ] 11.2 Create `analytics/analytics/flows/cdc_flow.py`
- [ ] 11.3 Add dead letter queue handling
- [ ] 11.4 Test CDC with live data

---

### Phase 12: Great Expectations (Data Quality)
**Goal**: Add data quality validation

**Tasks**:
- [ ] 12.1 Initialize Great Expectations project
- [ ] 12.2 Create `great_expectations.yml` configuration
- [ ] 12.3 Create bronze expectations suite
  - Column existence, uniqueness, not null
- [ ] 12.4 Create silver expectations suite
  - Valid message types, timestamp ordering
- [ ] 12.5 Create checkpoint configuration
- [ ] 12.6 Integrate into Prefect pipeline

---

### Phase 13: Docker Compose Deployment
**Goal**: Create production-ready Docker deployment

**Tasks**:
- [ ] 13.1 Update `analytics/Dockerfile` for all dependencies
- [ ] 13.2 Create `docker-compose.analytics.yml`
  - prefect-server, prefect-db, analytics-worker, metabase (no CDC)
- [ ] 13.3 Create volume configurations for DuckDB and Parquet data
- [ ] 13.4 Add health checks
- [ ] 13.5 Test full stack locally

---

### Phase 14: Metabase Setup
**Goal**: Configure Metabase for visualization

**Tasks**:
- [ ] 14.1 Configure DuckDB connection in Metabase
- [ ] 14.2 Create Developer Productivity dashboard
  - Session duration trends
  - Messages per session
  - Active coding hours heatmap
- [ ] 14.3 Create AI Interaction Patterns dashboard
  - Tool usage distribution
  - Success/failure rates
- [ ] 14.4 Create Project Insights dashboard
  - Activity by project
  - Code changes over time
- [ ] 14.5 Create sample questions/queries

---

### Phase 15: Documentation & Testing
**Goal**: Complete documentation and integration tests

**Tasks**:
- [ ] 15.1 Generate dbt documentation (`dbt docs generate`)
- [ ] 15.2 Add dbt docs to Docker deployment
- [ ] 15.3 Create end-to-end integration tests
- [ ] 15.4 Update main README with analytics instructions
- [ ] 15.5 Create runbook for common operations

---

## Critical Files to Create/Modify

| Path | Purpose |
|------|---------|
| `analytics/pyproject.toml` | Python project config |
| `analytics/Dockerfile` | Container definition |
| `analytics/analytics/extractor.py` | MongoDB extraction (with backfill) |
| `analytics/analytics/loader.py` | DuckDB loading |
| `analytics/analytics/config.py` | Configuration management |
| `analytics/analytics/flows/main_pipeline.py` | Prefect orchestration |
| `analytics/dbt/dbt_project.yml` | dbt configuration |
| `analytics/dbt/models/staging/*.sql` | Bronze layer |
| `analytics/dbt/models/intermediate/*.sql` | Silver layer |
| `analytics/dbt/models/marts/*.sql` | Gold layer |
| `analytics/docker-compose.analytics.yml` | Full stack deployment |
| `analytics/great_expectations/great_expectations.yml` | Data quality config |

---

## Dependencies

```
# Python (analytics/requirements.txt)
pymongo>=4.6
pyarrow>=15.0
pandas>=2.0
duckdb>=0.10
prefect>=2.14
great-expectations>=0.18
python-dotenv>=1.0

# dbt (installed via pip)
dbt-core>=1.7
dbt-duckdb>=1.7
```

---

## Estimated Effort by Phase

| Phase | Complexity | Notes |
|-------|------------|-------|
| 1-3 | Medium | Core extraction/loading infrastructure |
| 4-6 | Medium | dbt setup and staging |
| 7-9 | High | Data modeling requires careful design |
| 10 | Medium | Prefect orchestration |
| 11 | - | DEFERRED (CDC) |
| 12-13 | Medium | Quality + Deployment |
| 14-15 | Medium | Visualization + Docs |

---

## Success Criteria

- [ ] MongoDB data flows to DuckDB via Parquet
- [ ] dbt models pass all tests
- [ ] Metabase dashboards show meaningful metrics
- [ ] Pipeline runs reliably on schedule
- [ ] Great Expectations validates data quality
- [ ] Documentation enables self-service analytics
