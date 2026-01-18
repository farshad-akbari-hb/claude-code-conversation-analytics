# Claude Conversation Analytics Platform Specification

## Executive Summary

This document specifies an ELT (Extract-Load-Transform) analytics platform for Claude Code conversation logs. The platform extracts data from MongoDB, loads it into DuckDB via Apache Iceberg tables, transforms it using dbt, and visualizes insights through Metabase.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Component Specifications](#component-specifications)
4. [Data Model](#data-model)
5. [dbt Project Structure](#dbt-project-structure)
6. [Metrics & KPIs](#metrics--kpis)
7. [Deployment](#deployment)
8. [Observability](#observability)
9. [Data Quality](#data-quality)
10. [Documentation & Lineage](#documentation--lineage)
11. [Configuration](#configuration)
12. [Appendix](#appendix)

---

## Architecture Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ANALYTICS PLATFORM                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌───────────────┐    ┌──────────────┐    ┌──────────┐ │
│  │   MongoDB    │───▶│   Extractor   │───▶│   Iceberg    │───▶│  DuckDB  │ │
│  │ (Source DB)  │    │   (Python)    │    │   (Tables)   │    │  (OLAP)  │ │
│  └──────────────┘    └───────────────┘    └──────────────┘    └──────────┘ │
│         │                                                           │       │
│         │ CDC                                                       │       │
│         ▼                                                           ▼       │
│  ┌──────────────┐                                          ┌──────────────┐ │
│  │ Change       │                                          │     dbt      │ │
│  │ Stream       │──────────────────────────────────────────│  (Transform) │ │
│  │ Listener     │                                          └──────────────┘ │
│  └──────────────┘                                                  │        │
│                                                                    ▼        │
│  ┌──────────────┐    ┌───────────────┐    ┌──────────────────────────────┐ │
│  │   Prefect    │───▶│ Great         │───▶│          Metabase            │ │
│  │ Orchestrator │    │ Expectations  │    │       (Visualization)        │ │
│  └──────────────┘    └───────────────┘    └──────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **ELT over ETL**: Load raw data first, transform in-database for maximum flexibility
2. **Medallion Architecture**: Bronze → Silver → Gold layer separation
3. **Incremental Processing**: Upsert logic for efficient handling of late-arriving data
4. **Hybrid Triggering**: CDC for real-time metrics, batch for heavy aggregations
5. **Local-First**: All components run locally via Docker Compose

---

## Data Flow

### Hybrid Pipeline Trigger Strategy

```
┌─────────────────────────────────────────────────────────────────┐
│                      TRIGGER PATTERNS                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  REAL-TIME PATH (CDC - 1-5 min latency)                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ MongoDB  │───▶│ Change   │───▶│ Micro-   │───▶│ Staging  │  │
│  │ Change   │    │ Stream   │    │ Batch    │    │ Tables   │  │
│  │ Stream   │    │ Listener │    │ Process  │    │          │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│                                                                  │
│  BATCH PATH (Hourly/Daily)                                       │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │
│  │ Prefect  │───▶│ Full     │───▶│ Heavy    │───▶│ Gold     │  │
│  │ Schedule │    │ Extract  │    │ Agg.     │    │ Tables   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Pipeline Stages

| Stage | Component | Trigger | Latency | Description |
|-------|-----------|---------|---------|-------------|
| Extract | Python Extractor | CDC + Batch | 1-5 min | Read from MongoDB, output to Iceberg |
| Load | DuckDB Loader | After Extract | Immediate | Load Iceberg into raw tables |
| Transform | dbt | After Load | Minutes | Bronze → Silver → Gold |
| Validate | Great Expectations | After Transform | Seconds | Data quality checks |
| Visualize | Metabase | On Query | Real-time | Dashboard rendering |

---

## Component Specifications

### 1. Extractor Service (Python)

**Responsibility**: Extract data from MongoDB and write to Apache Iceberg tables

```python
# Pseudo-structure
class IcebergExtractor:
    """
    Handles both CDC and batch extraction from MongoDB.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.catalog_manager = IcebergCatalogManager(settings)

    def start_change_stream(self) -> None:
        """Listen to MongoDB change stream for real-time updates."""
        pass

    def extract(self, full_backfill: bool = False) -> int:
        """Full extraction with incremental filter."""
        pass

    def _write_to_iceberg(self, records: list, table: Table) -> int:
        """Write records to Iceberg table."""
        pass
```

**Key Features**:
- MongoDB Change Stream listener for CDC (1-5 minute micro-batches)
- Batch extraction with high-water mark tracking
- Iceberg output with date partitioning and ACID transactions
- Dead letter queue for failed extractions

**Configuration**:
```yaml
extractor:
  mongo_uri: ${MONGO_URI}
  database: claude_logs
  collection: entries
  output_dir: ./data/raw
  batch_size: 10000
  cdc_buffer_seconds: 60
  partition_by: date
```

### 2. DuckDB Loader

**Responsibility**: Load Iceberg tables into DuckDB database

```sql
-- Example loader pattern using DuckDB's Iceberg extension
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE TABLE raw.conversations AS
SELECT * FROM iceberg_scan('/data/iceberg/analytics/conversations');
```

**Upsert Logic for Late-Arriving Data**:
```sql
-- Merge pattern for handling duplicates
MERGE INTO raw.conversations AS target
USING (SELECT * FROM iceberg_scan('/data/iceberg/analytics/conversations')) AS source
ON target._id = source._id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### 3. dbt Project

**Responsibility**: Transform raw data through Bronze → Silver → Gold layers

**Project Structure**:
```
analytics/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/           # Bronze Layer
│   │   ├── stg_conversations.sql
│   │   ├── stg_messages.sql
│   │   └── stg_tool_calls.sql
│   ├── intermediate/      # Silver Layer
│   │   ├── int_messages_enriched.sql
│   │   ├── int_sessions_computed.sql
│   │   └── int_tool_usage.sql
│   └── marts/             # Gold Layer
│       ├── dim_projects.sql
│       ├── dim_sessions.sql
│       ├── dim_tools.sql
│       ├── dim_date.sql
│       ├── fact_messages.sql
│       ├── fact_tool_calls.sql
│       ├── fact_file_operations.sql
│       └── aggregates/
│           ├── agg_session_metrics.sql
│           ├── agg_tool_efficiency.sql
│           ├── agg_code_changes.sql
│           └── agg_daily_summary.sql
├── seeds/
│   └── tool_categories.csv
├── macros/
│   └── upsert_strategy.sql
├── tests/
│   └── custom/
└── snapshots/
```

### 4. Prefect Orchestrator

**Responsibility**: Orchestrate pipeline runs, handle dependencies, manage retries

```python
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract_from_mongo(since: datetime) -> Path:
    """Extract new data from MongoDB."""
    pass

@task
def load_to_duckdb() -> None:
    """Load Iceberg tables into DuckDB."""
    pass

@task
def run_dbt_models(select: str = None) -> None:
    """Execute dbt models."""
    pass

@task
def run_great_expectations() -> bool:
    """Validate data quality."""
    pass

@flow(name="analytics-pipeline")
def analytics_pipeline(full_refresh: bool = False):
    """Main analytics pipeline flow."""
    extract_to_iceberg(full_backfill=full_refresh)
    load_to_duckdb()
    run_dbt_models()
    run_great_expectations()
```

**Schedule Configuration**:
```python
# Batch jobs
analytics_pipeline.serve(
    name="hourly-analytics",
    cron="0 * * * *",  # Every hour
)

# CDC listener (separate deployment)
cdc_listener.serve(
    name="realtime-cdc",
    # Runs continuously
)
```

### 5. Great Expectations

**Responsibility**: Data quality validation

**Expectation Suites**:

```yaml
# expectations/bronze_conversations.json
expectations:
  - expectation_type: expect_column_to_exist
    kwargs:
      column: _id
  - expectation_type: expect_column_values_to_be_unique
    kwargs:
      column: _id
  - expectation_type: expect_column_values_to_not_be_null
    kwargs:
      column: timestamp
  - expectation_type: expect_column_values_to_be_of_type
    kwargs:
      column: timestamp
      type_: datetime64
```

**Custom Validations**:
- Valid tool names (from known set)
- Timestamp ordering within sessions
- Message role alternation (human/assistant)
- File path format validation

### 6. Metabase

**Responsibility**: Business intelligence and visualization

**Dashboard Categories**:
1. **Developer Productivity**
   - Session duration trends
   - Messages per session
   - Active coding hours heatmap

2. **AI Interaction Patterns**
   - Tool usage distribution
   - Success/failure rates
   - Conversation length distributions

3. **Project Insights**
   - Activity by project
   - Code changes over time
   - Feature request patterns

4. **Code Change Metrics**
   - Files modified per session
   - Edit patterns
   - Most-changed files

---

## Data Model

### Star Schema Design

```
                          ┌──────────────────┐
                          │    dim_date      │
                          ├──────────────────┤
                          │ date_key (PK)    │
                          │ date             │
                          │ day_of_week      │
                          │ hour             │
                          │ is_weekend       │
                          │ week_of_year     │
                          │ month            │
                          │ quarter          │
                          └────────┬─────────┘
                                   │
┌──────────────────┐    ┌─────────┴──────────┐    ┌──────────────────┐
│   dim_projects   │    │   fact_messages    │    │   dim_sessions   │
├──────────────────┤    ├────────────────────┤    ├──────────────────┤
│ project_key (PK) │◀───│ project_key (FK)   │───▶│ session_key (PK) │
│ project_path     │    │ session_key (FK)   │    │ session_id       │
│ project_name     │    │ date_key (FK)      │    │ start_time       │
│ first_seen       │    │ message_id (PK)    │    │ end_time         │
│ last_active      │    │ role               │    │ duration_seconds │
└──────────────────┘    │ content_length     │    │ message_count    │
                        │ timestamp          │    │ project_key (FK) │
┌──────────────────┐    │ token_count        │    └──────────────────┘
│    dim_tools     │    └────────────────────┘
├──────────────────┤               │
│ tool_key (PK)    │    ┌──────────┴──────────┐
│ tool_name        │    │  fact_tool_calls    │
│ tool_category    │◀───├────────────────────┤
│ description      │    │ tool_call_id (PK)  │
└──────────────────┘    │ message_key (FK)   │
                        │ tool_key (FK)      │
                        │ date_key (FK)      │
                        │ session_key (FK)   │
                        │ is_success         │
                        │ duration_ms        │
                        │ retry_count        │
                        └────────────────────┘

                        ┌────────────────────┐
                        │ fact_file_operations│
                        ├────────────────────┤
                        │ operation_id (PK)  │
                        │ tool_call_key (FK) │
                        │ file_path          │
                        │ operation_type     │
                        │ lines_changed      │
                        │ file_extension     │
                        └────────────────────┘
```

### Entity-Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ENTITY RELATIONSHIPS                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  PROJECT (1) ─────────────────────────────── (N) SESSION                │
│      │                                              │                    │
│      │                                              │                    │
│      │ (1)                                     (1)  │                    │
│      │                                              │                    │
│      ▼ (N)                                     (N)  ▼                    │
│  MESSAGE ◀──────────────────────────────────────────┘                   │
│      │                                                                   │
│      │ (1)                                                               │
│      │                                                                   │
│      ▼ (N)                                                               │
│  TOOL_CALL                                                               │
│      │                                                                   │
│      │ (1)                                                               │
│      │                                                                   │
│      ▼ (N)                                                               │
│  FILE_OPERATION                                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### Multiple Grain Tables

| Table | Grain | Use Case |
|-------|-------|----------|
| `fact_messages` | Per message | Detailed conversation analysis |
| `fact_tool_calls` | Per tool invocation | Tool usage patterns |
| `fact_file_operations` | Per file change | Code change analysis |
| `agg_session_metrics` | Per session | Productivity dashboards |
| `agg_daily_summary` | Per day | Trend analysis |
| `agg_hourly_activity` | Per hour | Usage pattern heatmaps |

---

## dbt Project Structure

### Model Naming Conventions (dbt Labs Style Guide)

| Layer | Prefix | Example |
|-------|--------|---------|
| Staging (Bronze) | `stg_` | `stg_conversations` |
| Intermediate (Silver) | `int_` | `int_messages_enriched` |
| Dimension (Gold) | `dim_` | `dim_projects` |
| Fact (Gold) | `fct_` | `fct_messages` |
| Aggregate | `agg_` | `agg_session_metrics` |

### Key Model Definitions

#### Staging Layer (Bronze)

```sql
-- models/staging/stg_conversations.sql
{{
    config(
        materialized='incremental',
        unique_key='_id',
        incremental_strategy='merge'
    )
}}

SELECT
    _id,
    type,
    timestamp,
    project_path,
    session_id,
    message,
    tool_use,
    _synced_at,
    -- Preserve raw JSON for future flexibility
    _raw_json
FROM {{ source('raw', 'conversations') }}

{% if is_incremental() %}
WHERE _synced_at > (SELECT MAX(_synced_at) FROM {{ this }})
{% endif %}
```

#### Intermediate Layer (Silver)

```sql
-- models/intermediate/int_messages_enriched.sql
{{
    config(
        materialized='incremental',
        unique_key='message_id'
    )
}}

WITH base AS (
    SELECT * FROM {{ ref('stg_conversations') }}
    WHERE type = 'message'
),

enriched AS (
    SELECT
        _id AS message_id,
        session_id,
        project_path,
        message.role AS role,
        message.content AS content,
        LENGTH(message.content) AS content_length,
        -- Simple keyword classification
        CASE
            WHEN content ILIKE '%bug%' OR content ILIKE '%fix%' THEN 'bug_fix'
            WHEN content ILIKE '%feature%' OR content ILIKE '%add%' THEN 'feature'
            WHEN content ILIKE '%refactor%' THEN 'refactor'
            WHEN content ILIKE '%test%' THEN 'testing'
            ELSE 'other'
        END AS task_category,
        timestamp,
        DATE(timestamp) AS date_key,
        EXTRACT(HOUR FROM timestamp) AS hour_of_day,
        EXTRACT(DOW FROM timestamp) AS day_of_week
    FROM base
)

SELECT * FROM enriched
```

#### Gold Layer (Dimensions)

```sql
-- models/marts/dim_projects.sql
{{
    config(
        materialized='table'
    )
}}

WITH project_stats AS (
    SELECT
        project_path,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_active,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(*) AS total_messages
    FROM {{ ref('int_messages_enriched') }}
    GROUP BY project_path
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['project_path']) }} AS project_key,
    project_path,
    SPLIT_PART(project_path, '/', -1) AS project_name,
    first_seen,
    last_active,
    total_sessions,
    total_messages
FROM project_stats
```

#### Gold Layer (Facts)

```sql
-- models/marts/fct_tool_calls.sql
{{
    config(
        materialized='incremental',
        unique_key='tool_call_id',
        incremental_strategy='merge'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['_id', 'tool_index']) }} AS tool_call_id,
    m.message_id,
    p.project_key,
    s.session_key,
    d.date_key,
    t.tool_key,
    tc.tool_name,
    tc.is_success,
    tc.duration_ms,
    tc.retry_count,
    tc.timestamp
FROM {{ ref('stg_tool_calls') }} tc
JOIN {{ ref('int_messages_enriched') }} m ON tc.message_id = m.message_id
JOIN {{ ref('dim_projects') }} p ON m.project_path = p.project_path
JOIN {{ ref('dim_sessions') }} s ON m.session_id = s.session_id
JOIN {{ ref('dim_tools') }} t ON tc.tool_name = t.tool_name
JOIN {{ ref('dim_date') }} d ON DATE(tc.timestamp) = d.date

{% if is_incremental() %}
WHERE tc._synced_at > (SELECT MAX(_synced_at) FROM {{ this }})
{% endif %}
```

#### Aggregate Tables

```sql
-- models/marts/aggregates/agg_session_metrics.sql
{{
    config(
        materialized='table'
    )
}}

SELECT
    s.session_key,
    s.session_id,
    p.project_key,
    p.project_name,
    d.date_key,

    -- Session duration metrics
    s.duration_seconds,
    s.duration_seconds / 60.0 AS duration_minutes,

    -- Message metrics
    COUNT(DISTINCT CASE WHEN m.role = 'human' THEN m.message_id END) AS human_messages,
    COUNT(DISTINCT CASE WHEN m.role = 'assistant' THEN m.message_id END) AS assistant_messages,
    COUNT(DISTINCT m.message_id) AS total_messages,

    -- Tool metrics
    COUNT(DISTINCT tc.tool_call_id) AS total_tool_calls,
    COUNT(DISTINCT CASE WHEN tc.is_success THEN tc.tool_call_id END) AS successful_tool_calls,
    ROUND(
        COUNT(DISTINCT CASE WHEN tc.is_success THEN tc.tool_call_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT tc.tool_call_id), 0), 2
    ) AS tool_success_rate,

    -- Content metrics
    SUM(m.content_length) AS total_content_length,
    AVG(m.content_length) AS avg_message_length

FROM {{ ref('dim_sessions') }} s
JOIN {{ ref('dim_projects') }} p ON s.project_key = p.project_key
JOIN {{ ref('dim_date') }} d ON DATE(s.start_time) = d.date
LEFT JOIN {{ ref('fct_messages') }} m ON s.session_key = m.session_key
LEFT JOIN {{ ref('fct_tool_calls') }} tc ON s.session_key = tc.session_key
GROUP BY 1, 2, 3, 4, 5, 6, 7
```

---

## Metrics & KPIs

### Session Metrics

| Metric | Description | Formula |
|--------|-------------|---------|
| Session Duration | Time from first to last message | `MAX(timestamp) - MIN(timestamp)` |
| Messages per Session | Average message count | `COUNT(messages) / COUNT(sessions)` |
| Turns per Session | Human-AI exchange count | `COUNT(human_messages)` |
| Active Sessions | Sessions with activity in period | `COUNT(DISTINCT session_id) WHERE timestamp IN period` |

### Tool Efficiency Metrics

| Metric | Description | Formula |
|--------|-------------|---------|
| Tool Success Rate | Percentage of successful tool calls | `successful_calls / total_calls * 100` |
| Avg Tool Duration | Mean execution time per tool | `AVG(duration_ms) GROUP BY tool_name` |
| Retry Rate | Percentage requiring retries | `calls_with_retries / total_calls * 100` |
| Tool Distribution | Usage share by tool type | `COUNT(tool) / SUM(COUNT(tool))` |

### Code Change Metrics

| Metric | Description | Formula |
|--------|-------------|---------|
| Files Modified | Unique files changed per session | `COUNT(DISTINCT file_path)` |
| Lines Changed | Total line modifications | `SUM(lines_added + lines_removed)` |
| Edit Frequency | Edits per hour of session | `edit_count / session_duration_hours` |
| File Type Distribution | Changes by file extension | `COUNT(*) GROUP BY file_extension` |

### Productivity Metrics

| Metric | Description | Formula |
|--------|-------------|---------|
| Daily Active Hours | Hours with any activity | `COUNT(DISTINCT HOUR(timestamp))` |
| Peak Productivity Hours | Hours with most activity | `MODE(HOUR(timestamp))` |
| Task Completion Rate | Inferred from conversation patterns | Keyword analysis |
| Context Switching | Project changes per day | `COUNT(DISTINCT project_path)` |

---

## Deployment

### Docker Compose Stack

```yaml
# docker-compose.analytics.yml
version: '3.8'

services:
  # Prefect Server
  prefect-server:
    image: prefecthq/prefect:2-python3.11
    command: prefect server start --host 0.0.0.0
    ports:
      - "4200:4200"
    volumes:
      - prefect-data:/root/.prefect
    environment:
      - PREFECT_API_DATABASE_CONNECTION_URL=postgresql://prefect:prefect@prefect-db:5432/prefect
    depends_on:
      - prefect-db

  prefect-db:
    image: postgres:15
    environment:
      - POSTGRES_USER=prefect
      - POSTGRES_PASSWORD=prefect
      - POSTGRES_DB=prefect
    volumes:
      - prefect-postgres:/var/lib/postgresql/data

  # Analytics Worker
  analytics-worker:
    build:
      context: ./analytics
      dockerfile: Dockerfile
    command: prefect worker start --pool analytics-pool
    volumes:
      - ./analytics:/app
      - ./data:/data
      - duckdb-data:/duckdb
    environment:
      - MONGO_URI=${MONGO_URI}
      - DUCKDB_PATH=/duckdb/analytics.db
      - PREFECT_API_URL=http://prefect-server:4200/api
    depends_on:
      - prefect-server

  # CDC Listener
  cdc-listener:
    build:
      context: ./analytics
      dockerfile: Dockerfile
    command: python -m analytics.cdc_listener
    volumes:
      - ./data:/data
    environment:
      - MONGO_URI=${MONGO_URI}
    restart: unless-stopped

  # Metabase
  metabase:
    image: metabase/metabase:latest
    ports:
      - "3001:3000"
    volumes:
      - metabase-data:/metabase-data
      - duckdb-data:/duckdb:ro
    environment:
      - MB_DB_FILE=/metabase-data/metabase.db
    depends_on:
      - analytics-worker

  # Great Expectations (runs as part of pipeline, not always-on)
  # Included in analytics-worker image

volumes:
  prefect-data:
  prefect-postgres:
  duckdb-data:
  metabase-data:
```

### Directory Structure

```
claude-mongo-sync/
├── ... (existing sync service files)
│
└── analytics/                    # New analytics service
    ├── Dockerfile
    ├── pyproject.toml
    ├── requirements.txt
    │
    ├── analytics/               # Python package
    │   ├── __init__.py
    │   ├── extractor.py        # MongoDB extraction
    │   ├── loader.py           # DuckDB loading
    │   ├── cdc_listener.py     # Change stream listener
    │   └── flows/              # Prefect flows
    │       ├── __init__.py
    │       ├── main_pipeline.py
    │       └── cdc_flow.py
    │
    ├── dbt/                     # dbt project
    │   ├── dbt_project.yml
    │   ├── profiles.yml
    │   ├── models/
    │   │   ├── staging/
    │   │   ├── intermediate/
    │   │   └── marts/
    │   ├── seeds/
    │   ├── macros/
    │   ├── tests/
    │   └── snapshots/
    │
    ├── great_expectations/      # Data quality
    │   ├── great_expectations.yml
    │   ├── expectations/
    │   └── checkpoints/
    │
    ├── data/                    # Data files (gitignored)
    │   ├── iceberg/             # Iceberg warehouse
    │   └── dead_letter/
    │
    └── docker-compose.yml
```

### Port Mapping

| Service | Port | Description |
|---------|------|-------------|
| Sync Health | 9090 | Existing sync service |
| UI | 3000 | Existing Next.js UI |
| Metabase | 3001 | Analytics dashboards |
| Prefect UI | 4200 | Pipeline monitoring |

---

## Observability

### Prefect Native Monitoring

Prefect provides built-in observability:

1. **Flow Runs Dashboard**: Visual timeline of all pipeline executions
2. **Task States**: Track pending, running, completed, failed tasks
3. **Logs**: Centralized logging for all flow and task executions
4. **Notifications**: Configurable alerts for failures

### Key Metrics to Track

| Metric | Type | Description |
|--------|------|-------------|
| `extraction_records_count` | Counter | Records extracted per run |
| `extraction_duration_seconds` | Histogram | Time to complete extraction |
| `dbt_model_duration` | Histogram | Time per dbt model |
| `dead_letter_queue_size` | Gauge | Failed records awaiting retry |
| `pipeline_success_rate` | Gauge | Percentage of successful runs |

### Alerting Rules

```yaml
# prefect.yaml notifications section
notifications:
  - name: pipeline-failure
    condition: flow_run.state_name == "Failed"
    actions:
      - type: slack
        webhook_url: ${SLACK_WEBHOOK_URL}
      - type: email
        to: ${ALERT_EMAIL}
```

---

## Data Quality

### Great Expectations Suite

```python
# great_expectations/expectations/bronze_suite.py

from great_expectations.core import ExpectationSuite

suite = ExpectationSuite(
    expectation_suite_name="bronze_conversations"
)

# Column existence
suite.add_expectation(
    expectation_type="expect_column_to_exist",
    kwargs={"column": "_id"}
)

# Uniqueness
suite.add_expectation(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={"column": "_id"}
)

# Not null constraints
for col in ["_id", "timestamp", "type"]:
    suite.add_expectation(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": col}
    )

# Valid message types
suite.add_expectation(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "type",
        "value_set": ["message", "tool_use", "tool_result", "summary"]
    }
)

# Timestamp ordering (custom)
suite.add_expectation(
    expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
    kwargs={
        "column_A": "timestamp",
        "column_B": "previous_timestamp",
        "or_equal": True
    }
)
```

### dbt Tests

```yaml
# dbt/models/staging/schema.yml
version: 2

models:
  - name: stg_conversations
    description: Staged conversation entries from MongoDB
    columns:
      - name: _id
        description: Unique identifier
        tests:
          - unique
          - not_null

      - name: timestamp
        description: Event timestamp
        tests:
          - not_null

      - name: type
        description: Entry type
        tests:
          - accepted_values:
              values: ['message', 'tool_use', 'tool_result', 'summary']

      - name: session_id
        description: Session identifier
        tests:
          - relationships:
              to: ref('dim_sessions')
              field: session_id
```

### Dead Letter Queue

Failed records are written to a dead letter queue for investigation:

```
data/
└── dead_letter/
    ├── 2024-01-15/
    │   ├── extraction_failures.json
    │   └── validation_failures.json
    └── 2024-01-16/
        └── ...
```

---

## Documentation & Lineage

### dbt Documentation

Generate and serve dbt docs:

```bash
cd analytics/dbt
dbt docs generate
dbt docs serve --port 8080
```

### Data Dictionary

| Table | Column | Type | Description |
|-------|--------|------|-------------|
| `dim_projects` | `project_key` | VARCHAR | Surrogate key for project |
| `dim_projects` | `project_path` | VARCHAR | Full path to project directory |
| `dim_projects` | `project_name` | VARCHAR | Extracted project name |
| `dim_sessions` | `session_key` | VARCHAR | Surrogate key for session |
| `dim_sessions` | `duration_seconds` | INTEGER | Session duration in seconds |
| `fct_messages` | `message_id` | VARCHAR | Unique message identifier |
| `fct_messages` | `role` | VARCHAR | 'human' or 'assistant' |
| `fct_messages` | `content_length` | INTEGER | Character count of message |
| `fct_tool_calls` | `tool_name` | VARCHAR | Name of tool invoked |
| `fct_tool_calls` | `is_success` | BOOLEAN | Whether tool call succeeded |

### Field-Level Lineage

```
Source: MongoDB.claude_logs.entries
  └── _id → stg_conversations._id → int_messages_enriched.message_id → fct_messages.message_id
  └── message.content → stg_conversations.content → int_messages_enriched.content
  └── timestamp → stg_conversations.timestamp → dim_date.date (derived)
  └── tool_use.* → stg_tool_calls.* → fct_tool_calls.*
```

### ER Diagram (Mermaid)

```mermaid
erDiagram
    dim_projects ||--o{ dim_sessions : contains
    dim_sessions ||--o{ fct_messages : contains
    fct_messages ||--o{ fct_tool_calls : triggers
    fct_tool_calls ||--o{ fct_file_operations : performs
    dim_tools ||--o{ fct_tool_calls : categorizes
    dim_date ||--o{ fct_messages : occurs_on
    dim_date ||--o{ fct_tool_calls : occurs_on

    dim_projects {
        varchar project_key PK
        varchar project_path
        varchar project_name
        timestamp first_seen
        timestamp last_active
    }

    dim_sessions {
        varchar session_key PK
        varchar session_id
        varchar project_key FK
        timestamp start_time
        timestamp end_time
        integer duration_seconds
    }

    dim_tools {
        varchar tool_key PK
        varchar tool_name
        varchar tool_category
    }

    dim_date {
        date date_key PK
        date date
        integer day_of_week
        integer hour
        boolean is_weekend
    }

    fct_messages {
        varchar message_id PK
        varchar session_key FK
        varchar project_key FK
        date date_key FK
        varchar role
        integer content_length
        timestamp timestamp
    }

    fct_tool_calls {
        varchar tool_call_id PK
        varchar message_key FK
        varchar tool_key FK
        varchar session_key FK
        date date_key FK
        boolean is_success
        integer duration_ms
    }

    fct_file_operations {
        varchar operation_id PK
        varchar tool_call_key FK
        varchar file_path
        varchar operation_type
        integer lines_changed
    }
```

---

## Configuration

### Environment Variables

```bash
# .env.analytics
# MongoDB Source
MONGO_URI=mongodb://localhost:27017
MONGO_DB=claude_logs
MONGO_COLLECTION=entries

# DuckDB Target
DUCKDB_PATH=/duckdb/analytics.db

# Iceberg Storage
ICEBERG_WAREHOUSE_PATH=/data/iceberg
ICEBERG_CATALOG_NAME=default
ICEBERG_NAMESPACE=analytics
ICEBERG_TABLE_NAME=conversations
DEAD_LETTER_DIR=/data/dead_letter

# Pipeline Settings
BATCH_SIZE=10000
CDC_BUFFER_SECONDS=60
BACKFILL_ALL_HISTORICAL=true

# Prefect
PREFECT_API_URL=http://prefect-server:4200/api

# Alerting (optional)
SLACK_WEBHOOK_URL=
ALERT_EMAIL=
```

### dbt Profiles

```yaml
# dbt/profiles.yml
analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /duckdb/analytics.db
      threads: 4

    prod:
      type: duckdb
      path: /duckdb/analytics.db
      threads: 8
```

### Great Expectations Config

```yaml
# great_expectations/great_expectations.yml
config_version: 3.0
datasources:
  duckdb_datasource:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: duckdb:////duckdb/analytics.db
    data_connectors:
      default_inferred_data_connector:
        class_name: InferredAssetSqlDataConnector

stores:
  expectations_store:
    class_name: ExpectationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: expectations/

  validations_store:
    class_name: ValidationsStore
    store_backend:
      class_name: TupleFilesystemStoreBackend
      base_directory: validations/

checkpoints:
  pipeline_checkpoint:
    config_version: 1.0
    class_name: SimpleCheckpoint
    run_name_template: "%Y%m%d-%H%M%S-pipeline-run"
```

---

## Appendix

### A. Tool Categories Seed

```csv
# dbt/seeds/tool_categories.csv
tool_name,tool_category,description
Read,file_operations,Read file contents
Write,file_operations,Write file contents
Edit,file_operations,Edit file contents
Bash,shell,Execute shell commands
Glob,search,Search files by pattern
Grep,search,Search file contents
Task,agent,Spawn sub-agent
WebFetch,network,Fetch web content
WebSearch,network,Search the web
```

### B. Backfill Strategy

For initial deployment with full historical backfill:

1. **Phase 1**: Extract all MongoDB documents to Iceberg (may take hours for large datasets)
2. **Phase 2**: Load Iceberg into DuckDB raw tables
3. **Phase 3**: Run full dbt build (no incremental filters)
4. **Phase 4**: Run Great Expectations validation
5. **Phase 5**: Enable CDC listener for ongoing updates
6. **Phase 6**: Switch dbt to incremental mode

```bash
# Backfill commands
python -m analytics.cli extract --full-backfill
python -m analytics.cli load
dbt run --full-refresh
great_expectations checkpoint run backfill_checkpoint
```

### C. Keyword Classification Rules

```python
# Task category classification patterns
TASK_PATTERNS = {
    'bug_fix': [
        r'\bbug\b', r'\bfix\b', r'\berror\b', r'\bissue\b',
        r'\bbroken\b', r'\bfailing\b', r'\bcrash\b'
    ],
    'feature': [
        r'\badd\b', r'\bcreate\b', r'\bimplement\b', r'\bnew\b',
        r'\bfeature\b', r'\bbuild\b'
    ],
    'refactor': [
        r'\brefactor\b', r'\brestructure\b', r'\breorganize\b',
        r'\bclean\s*up\b', r'\bsimplify\b'
    ],
    'testing': [
        r'\btest\b', r'\bspec\b', r'\bvalidat\b', r'\bverif\b'
    ],
    'documentation': [
        r'\bdoc\b', r'\breadme\b', r'\bcomment\b', r'\bexplain\b'
    ]
}
```

### D. Sample Metabase Queries

```sql
-- Daily productivity summary
SELECT
    d.date,
    COUNT(DISTINCT s.session_key) AS sessions,
    SUM(s.duration_minutes) AS total_minutes,
    SUM(s.total_messages) AS messages,
    ROUND(AVG(s.tool_success_rate), 2) AS avg_tool_success
FROM agg_session_metrics s
JOIN dim_date d ON s.date_key = d.date_key
WHERE d.date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY d.date
ORDER BY d.date DESC;

-- Top tools by usage
SELECT
    t.tool_name,
    t.tool_category,
    COUNT(*) AS call_count,
    ROUND(AVG(tc.duration_ms), 2) AS avg_duration_ms,
    ROUND(SUM(CASE WHEN tc.is_success THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate
FROM fct_tool_calls tc
JOIN dim_tools t ON tc.tool_key = t.tool_key
GROUP BY t.tool_name, t.tool_category
ORDER BY call_count DESC
LIMIT 20;

-- Hourly activity heatmap
SELECT
    d.day_of_week,
    d.hour,
    COUNT(*) AS activity_count
FROM fct_messages m
JOIN dim_date d ON m.date_key = d.date_key
GROUP BY d.day_of_week, d.hour
ORDER BY d.day_of_week, d.hour;
```

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-01-02 | Claude | Initial specification |
