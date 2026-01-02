# Claude Analytics Platform - Architecture Document

> **Version**: 1.0.0
> **Last Updated**: January 2026
> **Status**: Production

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Overview](#2-system-overview)
3. [Data Architecture](#3-data-architecture)
4. [Component Architecture](#4-component-architecture)
5. [Infrastructure Architecture](#5-infrastructure-architecture)
6. [Data Model](#6-data-model)
7. [Orchestration & Scheduling](#7-orchestration--scheduling)
8. [Data Quality Framework](#8-data-quality-framework)
9. [Deployment Architecture](#9-deployment-architecture)
10. [Security Architecture](#10-security-architecture)
11. [Operational Procedures](#11-operational-procedures)
12. [Design Patterns & Principles](#12-design-patterns--principles)

---

## 1. Executive Summary

The **Claude Analytics Platform** is an enterprise-grade ELT (Extract, Load, Transform) analytics system that processes Claude Code conversation logs from MongoDB into a DuckDB OLAP database, applying dimensional modeling via dbt, and visualizing insights through Metabase.

### Key Capabilities

| Capability | Technology | Purpose |
|------------|------------|---------|
| **Extraction** | Python + PyMongo | Extract conversation logs from MongoDB |
| **Storage** | Parquet + DuckDB | Columnar storage for OLAP workloads |
| **Transformation** | dbt | Medallion architecture (Bronze → Silver → Gold) |
| **Orchestration** | Prefect 2.x | Scheduled and ad-hoc pipeline execution |
| **Data Quality** | Great Expectations | Validation at multiple pipeline stages |
| **Visualization** | Metabase | Self-service BI dashboards |

### Technology Stack

```
┌─────────────────────────────────────────────────────────────┐
│                    VISUALIZATION LAYER                       │
│                  Metabase (Custom DuckDB)                    │
├─────────────────────────────────────────────────────────────┤
│                   TRANSFORMATION LAYER                       │
│         dbt-core + dbt-duckdb (Medallion Architecture)      │
├─────────────────────────────────────────────────────────────┤
│                      STORAGE LAYER                           │
│              DuckDB (OLAP) + Parquet Files                  │
├─────────────────────────────────────────────────────────────┤
│                   ORCHESTRATION LAYER                        │
│              Prefect 2.19 + PostgreSQL Backend              │
├─────────────────────────────────────────────────────────────┤
│                    EXTRACTION LAYER                          │
│           Python 3.11 + PyMongo + PyArrow                   │
├─────────────────────────────────────────────────────────────┤
│                      SOURCE LAYER                            │
│               MongoDB (Conversation Logs)                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 2. System Overview

### 2.1 High-Level Data Flow

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        MONGO[(MongoDB<br/>conversations)]
    end

    subgraph Extraction["Extraction Layer"]
        EXT[MongoExtractor<br/>Python]
        HWM[High Water Mark<br/>Tracker]
    end

    subgraph Storage["Landing Zone"]
        PARQUET[("Parquet Files<br/>Date Partitioned")]
    end

    subgraph Loading["Loading Layer"]
        LOADER[DuckDBLoader<br/>Python]
    end

    subgraph Warehouse["Data Warehouse"]
        DUCK[(DuckDB<br/>analytics.db)]
    end

    subgraph Transform["Transformation Layer"]
        DBT[dbt Build<br/>Medallion Architecture]
    end

    subgraph Quality["Data Quality"]
        GE[Great Expectations<br/>Validation]
    end

    subgraph Presentation["Presentation Layer"]
        META[Metabase<br/>Dashboards]
        DOCS[dbt Docs<br/>Data Catalog]
    end

    subgraph Orchestration["Orchestration"]
        PREFECT[Prefect Server<br/>Scheduler]
        WORKER[Prefect Worker<br/>Executor]
    end

    MONGO -->|Incremental Extract| EXT
    EXT <-->|Track Progress| HWM
    EXT -->|Write| PARQUET
    PARQUET -->|Read| LOADER
    LOADER -->|Upsert| DUCK
    DUCK -->|Source| DBT
    DBT -->|Transform| DUCK
    DUCK -->|Validate| GE
    DUCK -->|Query| META
    DBT -->|Generate| DOCS

    PREFECT -->|Schedule| WORKER
    WORKER -->|Execute| EXT
    WORKER -->|Execute| LOADER
    WORKER -->|Execute| DBT
```

### 2.2 Component Interaction Diagram

```mermaid
sequenceDiagram
    participant P as Prefect Scheduler
    participant W as Worker
    participant E as MongoExtractor
    participant M as MongoDB
    participant F as Parquet Files
    participant L as DuckDBLoader
    participant D as DuckDB
    participant T as dbt
    participant G as Great Expectations

    P->>W: Trigger analytics_pipeline

    rect rgb(200, 230, 200)
        Note over W,F: Extraction Phase
        W->>E: extract_task(full_backfill=False)
        E->>E: Load high water mark
        E->>M: Query (ingestedAt > last_run)
        M-->>E: Documents (batched)
        E->>F: Write Parquet (date partitioned)
        E->>E: Update high water mark
        E-->>W: Return stats
    end

    rect rgb(200, 200, 230)
        Note over W,D: Loading Phase
        W->>L: load_task(extraction_stats)
        L->>F: Read Parquet (glob)
        L->>D: Upsert raw.conversations
        L-->>W: Return stats
    end

    rect rgb(230, 200, 200)
        Note over W,D: Transformation Phase
        W->>T: transform_task(load_stats)
        T->>D: Build staging models
        T->>D: Build intermediate models
        T->>D: Build mart models
        T-->>W: Return success
    end

    rect rgb(230, 230, 200)
        Note over W,G: Quality Validation
        W->>G: validate(bronze, silver)
        G->>D: Query tables
        G-->>W: Validation results
    end
```

---

## 3. Data Architecture

### 3.1 Medallion Architecture (Bronze → Silver → Gold)

```mermaid
flowchart LR
    subgraph Bronze["BRONZE LAYER<br/>(Raw Zone)"]
        direction TB
        RAW[(raw.conversations)]
        STG1[stg_conversations]
        STG2[stg_messages]
        STG3[stg_tool_calls]
    end

    subgraph Silver["SILVER LAYER<br/>(Cleaned Zone)"]
        direction TB
        INT1[int_messages_enriched]
        INT2[int_tool_usage]
        INT3[int_sessions_computed]
    end

    subgraph Gold["GOLD LAYER<br/>(Business Zone)"]
        direction TB
        subgraph Dimensions
            DIM1[dim_date]
            DIM2[dim_sessions]
            DIM3[dim_projects]
            DIM4[dim_tools]
        end
        subgraph Facts
            FCT1[fct_messages]
            FCT2[fct_tool_calls]
            FCT3[fct_file_operations]
        end
        subgraph Aggregates
            AGG1[agg_daily_summary]
            AGG2[agg_hourly_activity]
            AGG3[agg_session_metrics]
            AGG4[agg_tool_efficiency]
            AGG5[agg_code_changes]
        end
    end

    RAW --> STG1
    RAW --> STG2
    RAW --> STG3

    STG1 --> INT1
    STG2 --> INT1
    STG3 --> INT2
    STG1 --> INT3

    INT1 --> FCT1
    INT2 --> FCT2
    INT3 --> DIM2

    FCT1 --> AGG1
    FCT1 --> AGG2
    DIM2 --> AGG3
    FCT2 --> AGG4
    FCT3 --> AGG5
```

### 3.2 Layer Responsibilities

| Layer | Purpose | Materialization | Tests |
|-------|---------|-----------------|-------|
| **Bronze (Staging)** | Type casting, null handling, basic cleaning | VIEW | Schema tests |
| **Silver (Intermediate)** | Business logic, enrichment, computed fields | VIEW | Data quality |
| **Gold (Marts)** | Dimensional models, aggregations | TABLE | Business rules |

### 3.3 Data Lineage

```mermaid
flowchart TB
    subgraph Source
        MONGO[(MongoDB)]
    end

    subgraph Landing
        PQ[("Parquet<br/>/data/raw/date=*/")]
    end

    subgraph Raw
        RC[raw.conversations]
    end

    subgraph Staging
        SC[stg_conversations]
        SM[stg_messages]
        ST[stg_tool_calls]
    end

    subgraph Intermediate
        IME[int_messages_enriched]
        ITU[int_tool_usage]
        ISC[int_sessions_computed]
    end

    subgraph Marts
        DD[dim_date]
        DS[dim_sessions]
        DP[dim_projects]
        DT[dim_tools]
        FM[fct_messages]
        FT[fct_tool_calls]
        FF[fct_file_operations]
    end

    subgraph Aggregates
        ADS[agg_daily_summary]
        AHA[agg_hourly_activity]
    end

    MONGO --> PQ
    PQ --> RC
    RC --> SC
    RC --> SM
    RC --> ST
    SC --> IME
    SM --> IME
    ST --> ITU
    SC --> ISC
    SM --> ISC
    IME --> FM
    ITU --> FT
    ISC --> DS
    FM --> ADS
    FM --> AHA
    DS --> ADS
```

---

## 4. Component Architecture

### 4.1 Python Package Structure

```
analytics/
├── __init__.py                 # Package initialization
├── config.py                   # Pydantic settings management
├── extractor.py               # MongoDB → Parquet extraction
├── loader.py                  # Parquet → DuckDB loading
├── quality.py                 # Great Expectations integration
├── cli.py                     # Typer CLI interface
└── flows/
    ├── __init__.py
    ├── main_pipeline.py       # Prefect flow definitions
    └── deployment.py          # Deployment helpers
```

### 4.2 Module Responsibility Matrix

```mermaid
classDiagram
    class Config {
        +MongoSettings mongo
        +DuckDBSettings duckdb
        +DataSettings data
        +PipelineSettings pipeline
        +get_settings() Settings
    }

    class MongoExtractor {
        -MongoClient client
        -HighWaterMark hwm
        +connect()
        +disconnect()
        +extract(full_backfill) list
        +full_extract() list
        +incremental_extract() list
    }

    class DocumentTransformer {
        +flatten_message(msg) dict
        +parse_timestamp(ts) datetime
        +transform(doc) dict
    }

    class HighWaterMark {
        -Path filepath
        +get() datetime
        +set(timestamp)
    }

    class DuckDBLoader {
        -Connection conn
        +connect()
        +disconnect()
        +create_database()
        +load_from_parquet(pattern) int
        +upsert_incremental(pattern) int
        +get_table_stats() dict
    }

    class DataQualityValidator {
        -DataContext context
        +validate_bronze() dict
        +validate_silver() dict
        +run_checkpoint(name) dict
        +get_data_docs_url() str
    }

    class CLI {
        +config()
        +extract(full_backfill)
        +load(source, full_refresh)
        +transform(models, full_refresh)
        +pipeline(skip_*, full_*)
        +validate(bronze, silver)
        +deploy()
    }

    Config --> MongoExtractor
    Config --> DuckDBLoader
    Config --> DataQualityValidator
    MongoExtractor --> DocumentTransformer
    MongoExtractor --> HighWaterMark
    CLI --> MongoExtractor
    CLI --> DuckDBLoader
    CLI --> DataQualityValidator
```

### 4.3 Extractor Component Detail

```mermaid
flowchart TB
    subgraph MongoExtractor
        direction TB
        CONNECT[connect<br/>Lazy MongoDB connection]
        QUERY[Query with<br/>timestamp filter]
        BATCH[Batch processing<br/>10K docs/batch]
        TRANSFORM[Transform documents<br/>Flatten messages]
        PARTITION[Date partitioning<br/>Derive from timestamp]
        WRITE[Write Parquet<br/>Snappy compression]
        HWM_UPDATE[Update high<br/>water mark]
    end

    subgraph DocumentTransformer
        FLATTEN[Flatten message<br/>String/Dict/Array]
        PARSE_TS[Parse timestamp<br/>Multiple formats]
        DERIVE[Derive metadata<br/>Partition date]
    end

    subgraph HighWaterMark
        READ[Read last timestamp<br/>From JSON file]
        SAVE[Save new timestamp<br/>To JSON file]
    end

    CONNECT --> QUERY
    READ --> QUERY
    QUERY --> BATCH
    BATCH --> TRANSFORM
    TRANSFORM --> FLATTEN
    TRANSFORM --> PARSE_TS
    TRANSFORM --> DERIVE
    FLATTEN --> PARTITION
    PARSE_TS --> PARTITION
    DERIVE --> PARTITION
    PARTITION --> WRITE
    WRITE --> HWM_UPDATE
    HWM_UPDATE --> SAVE
```

### 4.4 Loader Component Detail

```mermaid
flowchart TB
    subgraph DuckDBLoader
        direction TB
        INIT[Initialize connection<br/>Thread configuration]
        SCHEMA[Create schema<br/>IF NOT EXISTS raw]
        TABLE[Create table<br/>PRIMARY KEY _id]
        INDEX[Create indexes<br/>5 covering indexes]
        GLOB[Glob Parquet files<br/>Hive partitioning]
        UPSERT[Upsert data<br/>ON CONFLICT UPDATE]
        STATS[Generate statistics<br/>Row counts, dates]
    end

    subgraph Indexes
        I1[idx_project_id]
        I2[idx_session_id]
        I3[idx_date]
        I4[idx_type]
        I5[idx_timestamp]
    end

    INIT --> SCHEMA
    SCHEMA --> TABLE
    TABLE --> INDEX
    INDEX --> I1
    INDEX --> I2
    INDEX --> I3
    INDEX --> I4
    INDEX --> I5
    I1 --> GLOB
    I2 --> GLOB
    I3 --> GLOB
    I4 --> GLOB
    I5 --> GLOB
    GLOB --> UPSERT
    UPSERT --> STATS
```

---

## 5. Infrastructure Architecture

### 5.1 Docker Service Topology

```mermaid
flowchart TB
    subgraph DockerHost["Docker Host"]
        subgraph Network["analytics-network"]
            subgraph Prefect["Prefect Stack"]
                PS[prefect-server<br/>:4200]
                PDB[(prefect-db<br/>PostgreSQL)]
                PW[prefect-worker]
            end

            subgraph Analytics["Analytics Stack"]
                AW[analytics-worker<br/>Manual CLI]
            end

            subgraph BI["BI Stack"]
                MB[metabase<br/>:3001→3000]
                MDB[(metabase-db<br/>PostgreSQL)]
                DBT_DOCS[dbt-docs<br/>:8080]
            end
        end

        subgraph Volumes["Persistent Volumes"]
            V1[(prefect-db-data)]
            V2[(metabase-db-data)]
            V3[(analytics-data)]
            V4[(duckdb-data)]
        end
    end

    subgraph External["External Services"]
        MONGO[(MongoDB<br/>host.docker.internal:27017)]
    end

    PS --> PDB
    PS --> V1
    PW --> PS
    PW --> V3
    PW --> V4
    AW --> V3
    AW --> V4
    MB --> MDB
    MB --> V4
    MDB --> V2
    DBT_DOCS --> V4
    PW -.->|host.docker.internal| MONGO
    AW -.->|host.docker.internal| MONGO
```

### 5.2 Service Configuration Matrix

| Service | Image | Port | Healthcheck | Dependencies |
|---------|-------|------|-------------|--------------|
| prefect-server | prefecthq/prefect:2.19-python3.11 | 4200 | HTTP /api/health | prefect-db |
| prefect-db | postgres:15-alpine | 5432 (internal) | pg_isready | - |
| prefect-worker | Custom (Dockerfile) | - | - | prefect-server |
| analytics-worker | Custom (Dockerfile) | - | - | - |
| metabase | Custom (metabase/Dockerfile) | 3001→3000 | HTTP /api/health | metabase-db |
| metabase-db | postgres:15-alpine | 5432 (internal) | pg_isready | - |
| dbt-docs | Custom (Dockerfile) | 8080 | - | - |

### 5.3 Container Build Architecture

```mermaid
flowchart TB
    subgraph BuildStage["Build Stage"]
        BASE1[python:3.11-slim]
        APT1[build-essential, git]
        VENV[Create /opt/venv]
        DEPS[Install requirements<br/>+ dbt-core + dbt-duckdb]
    end

    subgraph RuntimeStage["Runtime Stage"]
        BASE2[python:3.11-slim]
        APT2[libgomp1, curl, git]
        COPY_VENV[Copy /opt/venv]
        USER[Create analytics user]
        DIRS[Create /data, /duckdb]
        DBT_DEPS[dbt deps]
        HEALTH[Healthcheck curl]
    end

    BASE1 --> APT1
    APT1 --> VENV
    VENV --> DEPS
    DEPS --> COPY_VENV
    BASE2 --> APT2
    APT2 --> COPY_VENV
    COPY_VENV --> USER
    USER --> DIRS
    DIRS --> DBT_DEPS
    DBT_DEPS --> HEALTH
```

---

## 6. Data Model

### 6.1 Entity Relationship Diagram

```mermaid
erDiagram
    DIM_DATE {
        date date_key PK
        int year
        int month
        int week
        int day
        string day_name
        boolean is_weekend
    }

    DIM_SESSIONS {
        string session_key PK
        string session_id
        string project_id FK
        timestamp session_start
        timestamp session_end
        int duration_seconds
        int message_count
        int tool_call_count
        string activity_level
    }

    DIM_PROJECTS {
        string project_key PK
        string project_id
        string project_name
        int total_sessions
        timestamp first_activity
        timestamp last_activity
    }

    DIM_TOOLS {
        string tool_key PK
        string tool_name
        string tool_category
        int total_usage
    }

    FCT_MESSAGES {
        string message_key PK
        string session_key FK
        date date_key FK
        string project_id FK
        string role
        string task_category
        int content_length
        int message_sequence
        boolean has_code_block
        timestamp created_at
    }

    FCT_TOOL_CALLS {
        string tool_call_key PK
        string session_key FK
        string tool_key FK
        date date_key FK
        int execution_time_ms
        string status
    }

    FCT_FILE_OPERATIONS {
        string operation_key PK
        string session_key FK
        date date_key FK
        string file_path
        string operation_type
    }

    DIM_DATE ||--o{ FCT_MESSAGES : date_key
    DIM_DATE ||--o{ FCT_TOOL_CALLS : date_key
    DIM_DATE ||--o{ FCT_FILE_OPERATIONS : date_key
    DIM_SESSIONS ||--o{ FCT_MESSAGES : session_key
    DIM_SESSIONS ||--o{ FCT_TOOL_CALLS : session_key
    DIM_SESSIONS ||--o{ FCT_FILE_OPERATIONS : session_key
    DIM_PROJECTS ||--o{ DIM_SESSIONS : project_id
    DIM_TOOLS ||--o{ FCT_TOOL_CALLS : tool_key
```

### 6.2 Raw Schema (Source)

```sql
-- raw.conversations
CREATE TABLE raw.conversations (
    _id VARCHAR PRIMARY KEY,           -- MongoDB ObjectId
    type VARCHAR,                       -- user, assistant, tool_use, tool_result
    session_id VARCHAR,                -- Session grouping
    project_id VARCHAR,                -- Project grouping
    timestamp TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE,
    extracted_at TIMESTAMP WITH TIME ZONE,
    message_role VARCHAR,
    message_content VARCHAR,
    message_raw VARCHAR,               -- JSON for complex messages
    source_file VARCHAR,
    date DATE                          -- Partition key
);

-- Indexes
CREATE INDEX idx_conversations_project_id ON raw.conversations(project_id);
CREATE INDEX idx_conversations_session_id ON raw.conversations(session_id);
CREATE INDEX idx_conversations_date ON raw.conversations(date);
CREATE INDEX idx_conversations_type ON raw.conversations(type);
CREATE INDEX idx_conversations_timestamp ON raw.conversations(timestamp);
```

### 6.3 Aggregate Models

| Model | Grain | Key Metrics |
|-------|-------|-------------|
| **agg_daily_summary** | 1 row per day | sessions, messages, tools, duration |
| **agg_hourly_activity** | 1 row per hour | activity count (for heatmaps) |
| **agg_session_metrics** | 1 row per session | duration, messages, productivity |
| **agg_tool_efficiency** | 1 row per tool | usage, success rate, avg time |
| **agg_code_changes** | 1 row per day | files modified, operations |

---

## 7. Orchestration & Scheduling

### 7.1 Prefect Flow Architecture

```mermaid
flowchart TB
    subgraph Flows["Prefect Flows"]
        MAIN[analytics_pipeline<br/>Main orchestration]
        SCHED[scheduled_pipeline<br/>Hourly wrapper]
    end

    subgraph Tasks["Prefect Tasks"]
        EXT[extract_task<br/>3 retries, 30s backoff]
        LOAD[load_task<br/>3 retries, 30s backoff]
        TRANS[transform_task<br/>2 retries, 60s backoff]
    end

    subgraph Parameters
        P1[full_backfill: bool]
        P2[full_refresh: bool]
        P3[skip_extract: bool]
        P4[skip_load: bool]
        P5[skip_transform: bool]
        P6[dbt_select: str]
    end

    SCHED -->|Calls| MAIN
    MAIN --> P1
    MAIN --> P2
    MAIN --> P3
    MAIN --> P4
    MAIN --> P5
    MAIN --> P6

    MAIN -->|1| EXT
    EXT -->|2| LOAD
    LOAD -->|3| TRANS

    EXT -->|skip_extract| LOAD
    LOAD -->|skip_load| TRANS
```

### 7.2 Deployment Schedule Matrix

| Deployment | Schedule | Parameters | Use Case |
|------------|----------|------------|----------|
| **hourly-analytics** | Every 1h | Defaults | Incremental sync |
| **daily-full-refresh** | 2:00 AM daily | full_refresh=true | Full rebuild |
| **adhoc-analytics** | Manual | Defaults | On-demand |
| **full-backfill** | Manual | full_backfill=true, full_refresh=true | Initial load |

### 7.3 Task Dependency Graph

```mermaid
flowchart LR
    START((Start)) --> CHECK_SKIP_EXT{skip_extract?}

    CHECK_SKIP_EXT -->|No| EXTRACT[extract_task]
    CHECK_SKIP_EXT -->|Yes| CHECK_SKIP_LOAD

    EXTRACT --> CHECK_SKIP_LOAD{skip_load?}

    CHECK_SKIP_LOAD -->|No| LOAD[load_task]
    CHECK_SKIP_LOAD -->|Yes| CHECK_SKIP_TRANS

    LOAD --> CHECK_SKIP_TRANS{skip_transform?}

    CHECK_SKIP_TRANS -->|No| TRANSFORM[transform_task]
    CHECK_SKIP_TRANS -->|Yes| END((End))

    TRANSFORM --> END
```

---

## 8. Data Quality Framework

### 8.1 Great Expectations Integration

```mermaid
flowchart TB
    subgraph Context["GE Context"]
        DS[DuckDB Datasource]
        EXP[(Expectation Store)]
        VAL[(Validation Store)]
        DOCS[Data Docs Site]
    end

    subgraph Suites["Expectation Suites"]
        BRONZE[bronze_expectations]
        SILVER[silver_expectations]
    end

    subgraph Checkpoints
        CP1[analytics_checkpoint]
    end

    subgraph Tables["Validated Tables"]
        T1[raw.conversations]
        T2[staging.stg_conversations]
    end

    DS --> T1
    DS --> T2
    BRONZE --> T1
    SILVER --> T2
    CP1 --> BRONZE
    CP1 --> SILVER
    VAL --> DOCS
```

### 8.2 Validation Rules

**Bronze Layer (raw.conversations)**:

| Expectation | Column | Rule |
|-------------|--------|------|
| not_null | _id, type, date, extracted_at | Required fields |
| unique | _id | Primary key |
| value_set | type | ['user', 'assistant', 'tool_use', 'tool_result', 'unknown'] |
| between | content_length | 0 to 100,000 |

**Silver Layer (stg_conversations)**:

| Expectation | Column | Rule |
|-------------|--------|------|
| not_null | conversation_id, entry_type | Cleaned fields |
| format | effective_timestamp | Valid timestamp |
| referential | session_id | Exists in sessions |

### 8.3 dbt Test Strategy

| Test Type | Location | Purpose |
|-----------|----------|---------|
| **Schema tests** | _schema.yml | Column types, nullability |
| **Unique tests** | _schema.yml | Primary key validation |
| **Relationship tests** | _schema.yml | Foreign key validation |
| **Custom tests** | tests/*.sql | Business rule validation |
| **Freshness** | _sources.yml | SLA monitoring |

---

## 9. Deployment Architecture

### 9.1 Environment Configuration

```mermaid
flowchart TB
    subgraph Environments
        DEV[Development<br/>Local Docker]
        PROD[Production<br/>Docker Compose]
    end

    subgraph ConfigSources["Configuration Sources"]
        ENV[.env.analytics]
        PY[pyproject.toml]
        DBT[dbt/profiles.yml]
        PF[prefect.yaml]
    end

    subgraph Settings["Pydantic Settings"]
        MONGO_S[MongoSettings]
        DUCK_S[DuckDBSettings]
        DATA_S[DataSettings]
        PIPE_S[PipelineSettings]
        PREF_S[PrefectSettings]
        DBT_S[DbtSettings]
        GE_S[GreatExpectationsSettings]
        LOG_S[LoggingSettings]
    end

    ENV --> MONGO_S
    ENV --> DUCK_S
    ENV --> DATA_S
    ENV --> PIPE_S
    ENV --> PREF_S
    ENV --> DBT_S
    ENV --> GE_S
    ENV --> LOG_S

    DBT --> DEV
    DBT --> PROD
```

### 9.2 Key Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | mongodb://localhost:27017 | MongoDB connection |
| `MONGO_DB` | claude_logs | Database name |
| `DUCKDB_PATH` | /duckdb/analytics.db | DuckDB file path |
| `DUCKDB_THREADS` | 4 (dev) / 8 (prod) | Parallelism |
| `BATCH_SIZE` | 10000 | Extraction batch size |
| `DBT_TARGET` | dev | dbt profile target |
| `PREFECT_API_URL` | http://localhost:4200/api | Prefect server |
| `LOG_LEVEL` | INFO | Logging verbosity |

---

## 10. Security Architecture

### 10.1 Security Layers

```mermaid
flowchart TB
    subgraph Container["Container Security"]
        USER[Non-root user<br/>analytics:analytics]
        FS[Filesystem permissions<br/>Restricted paths]
        NET[Network isolation<br/>Internal services]
    end

    subgraph Config["Configuration Security"]
        ENV[Environment variables<br/>Not hardcoded]
        PYDANTIC[Pydantic validation<br/>Type safety]
        SECRETS[Sensitive in .env<br/>Git ignored]
    end

    subgraph Data["Data Security"]
        SQL[Parameterized queries<br/>dbt templating]
        ACCESS[Database per service<br/>Minimal privileges]
    end

    Container --> Config
    Config --> Data
```

### 10.2 Security Controls

| Layer | Control | Implementation |
|-------|---------|----------------|
| **Container** | Non-root execution | USER analytics in Dockerfile |
| **Container** | Minimal base image | python:3.11-slim |
| **Network** | Internal services | Docker bridge network |
| **Config** | Secret management | Environment variables |
| **Config** | Type validation | Pydantic strict mode |
| **Data** | SQL injection prevention | dbt Jinja templating |
| **Data** | Credential storage | .env files (gitignored) |

---

## 11. Operational Procedures

### 11.1 Makefile Commands

```
┌─────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE                            │
├─────────────────────────────────────────────────────────────┤
│ make up              Start all services                     │
│ make up-prefect      Start Prefect only (no Metabase)       │
│ make down            Stop all services                      │
│ make rebuild         Rebuild with --no-cache                │
│ make fresh           Full reset (down → rebuild → up)       │
│ make logs            Follow analytics-worker logs           │
│ make shell           Interactive shell in worker            │
├─────────────────────────────────────────────────────────────┤
│                     DEPLOYMENTS                              │
├─────────────────────────────────────────────────────────────┤
│ make deploy          Build, restart worker, deploy flows    │
│ make status          List Prefect deployments               │
├─────────────────────────────────────────────────────────────┤
│                   PIPELINE EXECUTION                         │
├─────────────────────────────────────────────────────────────┤
│ make run-adhoc       Run incremental pipeline               │
│ make run-backfill    Run full historical backfill           │
│ make run-daily       Run daily full refresh                 │
│ make pipeline        Run locally (without Prefect)          │
└─────────────────────────────────────────────────────────────┘
```

### 11.2 CLI Commands

```bash
# Configuration
claude-analytics config              # Display settings

# Extraction
claude-analytics extract             # Incremental
claude-analytics extract --full-backfill  # Full history

# Loading
claude-analytics load                # Load from default path
claude-analytics load --full-refresh # Truncate and reload
claude-analytics load --stats        # Show table statistics

# Transformation
claude-analytics transform           # Build all models
claude-analytics transform --models "staging.*"  # Specific models
claude-analytics transform --full-refresh  # Rebuild incremental

# Full Pipeline
claude-analytics pipeline            # Run all steps
claude-analytics pipeline --skip-extract  # Skip extraction
claude-analytics pipeline --prefect  # Via Prefect

# Data Quality
claude-analytics validate --bronze   # Validate raw layer
claude-analytics validate --silver   # Validate staging
claude-analytics validate --build-docs  # Generate HTML

# Deployment
claude-analytics deploy              # Deploy to Prefect
```

### 11.3 Monitoring Endpoints

| Endpoint | Port | Purpose |
|----------|------|---------|
| `http://localhost:4200` | 4200 | Prefect UI |
| `http://localhost:3001` | 3001 | Metabase dashboards |
| `http://localhost:8080` | 8080 | dbt documentation |
| `http://localhost:4200/api/health` | 4200 | Prefect health |
| `http://localhost:3001/api/health` | 3001 | Metabase health |

---

## 12. Design Patterns & Principles

### 12.1 Applied Patterns

| Pattern | Implementation | Benefit |
|---------|----------------|---------|
| **Single Responsibility** | Separate modules (extractor, loader, quality) | Maintainability |
| **Lazy Initialization** | Connections created on first use | Reduced startup time |
| **Immutable Configuration** | @lru_cache on settings | Thread safety |
| **Repository Pattern** | HighWaterMark for state | Testability |
| **Retry with Backoff** | Prefect task retries | Resilience |
| **Incremental Processing** | High water mark + dbt incremental | Efficiency |
| **Medallion Architecture** | Bronze → Silver → Gold | Data quality |

### 12.2 Error Recovery Strategy

```mermaid
flowchart TB
    START[Pipeline Start] --> EXT_TRY[Extract]

    EXT_TRY -->|Success| LOAD_TRY[Load]
    EXT_TRY -->|Fail| EXT_RETRY{Retry<br/>≤3?}
    EXT_RETRY -->|Yes| EXT_WAIT[Wait 30s×2^n]
    EXT_WAIT --> EXT_TRY
    EXT_RETRY -->|No| FAIL[Pipeline Failed]

    LOAD_TRY -->|Success| TRANS_TRY[Transform]
    LOAD_TRY -->|Fail| LOAD_RETRY{Retry<br/>≤3?}
    LOAD_RETRY -->|Yes| LOAD_WAIT[Wait 30s×2^n]
    LOAD_WAIT --> LOAD_TRY
    LOAD_RETRY -->|No| FAIL

    TRANS_TRY -->|Success| SUCCESS[Pipeline Success]
    TRANS_TRY -->|Fail| TRANS_RETRY{Retry<br/>≤2?}
    TRANS_RETRY -->|Yes| TRANS_WAIT[Wait 60s×2^n]
    TRANS_WAIT --> TRANS_TRY
    TRANS_RETRY -->|No| FAIL
```

### 12.3 Scalability Considerations

| Aspect | Current | Scaling Path |
|--------|---------|--------------|
| **Data Volume** | Single DuckDB file | Partitioned tables |
| **Extraction** | Sequential batches | Parallel workers |
| **Transformation** | Single dbt process | dbt Cloud / parallel |
| **Orchestration** | Single work pool | Multiple pools |
| **Visualization** | Single Metabase | Load balancer |

---

## Appendix A: File Inventory

```
analytics/
├── analytics/                 # Python package
│   ├── __init__.py
│   ├── config.py             # Settings management
│   ├── extractor.py          # MongoDB extraction
│   ├── loader.py             # DuckDB loading
│   ├── quality.py            # Great Expectations
│   ├── cli.py                # CLI interface
│   └── flows/                # Prefect flows
│       ├── __init__.py
│       ├── main_pipeline.py
│       └── deployment.py
├── dbt/                      # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/          # Bronze layer
│       ├── intermediate/     # Silver layer
│       └── marts/            # Gold layer
│           └── aggregates/
├── great_expectations/       # Data quality
│   ├── great_expectations.yml
│   ├── expectations/
│   └── checkpoints/
├── metabase/                 # Custom Metabase
│   └── Dockerfile
├── tests/                    # Python tests
│   └── test_extractor.py
├── Dockerfile               # Multi-stage build
├── docker-compose.analytics.yml
├── Makefile                 # Operations
├── prefect.yaml             # Deployment config
├── pyproject.toml           # Package metadata
├── requirements.txt         # Dependencies
└── README.md                # Documentation
```

---

## Appendix B: Quick Reference

### Start Development Environment
```bash
make up
make deploy
make run-backfill  # First time only
```

### Daily Operations
```bash
make status        # Check deployments
make logs          # Monitor execution
```

### Troubleshooting
```bash
make shell         # Enter container
claude-analytics config  # Verify settings
claude-analytics validate --bronze --silver  # Check data quality
```

---

*Document generated for Claude Analytics Platform v1.0.0*
