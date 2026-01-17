# Claude Analytics dbt Project

This dbt project transforms raw conversation data into analytics-ready models using a medallion architecture (Bronze → Silver → Gold).

## Architecture

```
raw.conversations (Source - loaded from Iceberg)
        │
        ▼
    [Staging/Bronze] ── Views
    stg_conversations ──┬── stg_messages
                        └── stg_tool_calls
        │
        ▼
    [Intermediate/Silver] ── Views
    int_messages_enriched
    int_sessions_computed
    int_tool_usage
        │
        ▼
    [Marts/Gold] ── Tables
    ┌─────────────┐     ┌─────────────────┐
    │ Dimensions  │     │     Facts       │
    ├─────────────┤     ├─────────────────┤
    │ dim_date    │     │ fct_messages    │
    │ dim_projects│     │ fct_tool_calls  │
    │ dim_sessions│     │ fct_file_ops    │
    │ dim_tools   │     └─────────────────┘
    └─────────────┘
            │
            ▼
        [Aggregates] ── Tables
    agg_daily_summary
    agg_hourly_activity
    agg_session_metrics
    agg_tool_efficiency
    agg_code_changes
```

## Quick Start

```bash
# Install dependencies
dbt deps

# Run all models (seeds + models + tests)
dbt build

# Run only models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve --port 8080
```

## Project Structure

```
dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connections (dev/prod/test)
├── packages.yml             # External dbt packages
├── seeds/                   # Static CSV reference data
│   ├── _seeds.yml
│   └── tool_categories.csv
└── models/
    ├── staging/             # Bronze layer
    │   ├── _sources.yml     # Source definitions + freshness
    │   ├── _schema.yml      # Tests & documentation
    │   ├── stg_conversations.sql
    │   ├── stg_messages.sql
    │   └── stg_tool_calls.sql
    ├── intermediate/        # Silver layer
    │   ├── _schema.yml
    │   ├── int_messages_enriched.sql
    │   ├── int_sessions_computed.sql
    │   └── int_tool_usage.sql
    └── marts/               # Gold layer
        ├── _schema_dimensions.yml
        ├── _schema_facts.yml
        ├── dim_*.sql        # Dimension tables
        ├── fct_*.sql        # Fact tables
        └── aggregates/      # Pre-computed rollups
            ├── _schema.yml
            └── agg_*.sql
```

## Commands

```bash
# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run with full refresh (rebuild all tables)
dbt run --full-refresh

# Run specific model and its upstream dependencies
dbt run --select +fct_messages

# Run specific model and its downstream dependents
dbt run --select dim_sessions+

# Run seeds (static CSV data)
dbt seed

# Run tests
dbt test
dbt test --select staging

# Check source freshness
dbt source freshness

# Compile SQL without executing
dbt compile
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PATH` | `/duckdb/analytics.db` | Path to DuckDB database |
| `DBT_TARGET` | `dev` | Profile target (dev/prod/test) |

### Profiles

```yaml
# profiles.yml
claude_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      threads: 4
    prod:
      type: duckdb
      path: "{{ env_var('DUCKDB_PATH') }}"
      threads: 8
    test:
      type: duckdb
      path: ":memory:"
      threads: 1
```

## Model Layers

### Staging (Bronze)

| Model | Source | Responsibility |
|-------|--------|----------------|
| `stg_conversations` | raw.conversations | Rename columns, coalesce nulls, add boolean flags |
| `stg_messages` | stg_conversations | Filter to user/assistant messages, add sequencing |
| `stg_tool_calls` | stg_conversations | Filter to tool_use/tool_result, parse tool names |

**Characteristics:**
- Materialized as **views** for quick iteration
- Minimal transformation logic
- Type casting and null handling
- Foundation for all downstream models

### Intermediate (Silver)

| Model | Sources | Responsibility |
|-------|---------|----------------|
| `int_sessions_computed` | stg_* | Session-level aggregates, duration, message counts |
| `int_messages_enriched` | stg_messages | Task categories, code detection, response timing |
| `int_tool_usage` | stg_tool_calls | Tool classification, file operation detection |

**Characteristics:**
- Materialized as **views**
- Business logic and enrichment
- Joins between staging models
- Computed fields and classifications

### Marts (Gold)

| Type | Models | Purpose |
|------|--------|---------|
| **Dimensions** | dim_date, dim_sessions, dim_projects, dim_tools | Descriptive attributes for filtering/grouping |
| **Facts** | fct_messages, fct_tool_calls, fct_file_operations | Measurable events with foreign keys |
| **Aggregates** | agg_daily_summary, agg_hourly_activity, etc. | Pre-computed metrics for dashboards |

**Characteristics:**
- Materialized as **tables** for performance
- Star schema design for BI tools
- Optimized for Metabase queries

## Materialization Strategy

| Layer | Materialization | Rationale |
|-------|-----------------|-----------|
| Staging | `view` | Always fresh, no storage cost, quick iteration |
| Intermediate | `view` | Recomputed on query, flexibility |
| Marts | `table` | Query performance, stable for Metabase |
| Aggregates | `table` | Dashboard performance, avoid expensive GROUP BYs |

## Testing

### Schema Tests

Defined in `_schema.yml` files:

```yaml
models:
  - name: stg_conversations
    columns:
      - name: conversation_id
        tests:
          - unique
          - not_null
      - name: entry_type
        tests:
          - accepted_values:
              values: ['user', 'assistant', 'tool_use', 'tool_result', ...]
```

### Source Freshness

```yaml
# _sources.yml
sources:
  - name: raw
    freshness:
      warn_after: {count: 24, period: hour}
      error_after: {count: 48, period: hour}
    loaded_at_field: extracted_at
```

Check with: `dbt source freshness`

## Output Schemas

After `dbt build`, DuckDB contains:

```sql
-- List schemas
SHOW SCHEMAS;

-- raw          (from Iceberg loader)
-- staging      (stg_* views)
-- intermediate (int_* views)
-- marts        (dim_*, fct_*, agg_* tables)
-- seeds        (static reference data)

-- Example queries
SELECT * FROM marts.agg_daily_summary ORDER BY date_key DESC LIMIT 7;
SELECT * FROM marts.dim_sessions WHERE activity_level = 'heavy';
SELECT COUNT(*) FROM staging.stg_conversations;
```

## Pipeline Integration

dbt runs as the final step in the Prefect pipeline:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  extract_task   │───▶│   load_task     │───▶│ transform_task  │
│ MongoDB→Iceberg │    │ Iceberg→DuckDB  │    │   dbt build     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

The pipeline automatically:
1. Pauses Metabase (releases DuckDB lock)
2. Runs `dbt build --target prod`
3. Resumes Metabase

## Seeds

| Seed | Purpose |
|------|---------|
| `tool_categories.csv` | Maps tool names to categories (file_op, search, shell, etc.) |

Load with: `dbt seed`

## Variables

Defined in `dbt_project.yml`:

```yaml
vars:
  start_date: '2024-01-01'
  end_date: '2099-12-31'
  task_patterns:
    bug_fix: ['bug', 'fix', 'error', 'issue']
    feature: ['add', 'create', 'implement', 'new']
    refactor: ['refactor', 'restructure', 'clean up']
```

Use in SQL: `{{ var('task_patterns')['bug_fix'] }}`

## Troubleshooting

```bash
# Debug a model
dbt run --select my_model --debug

# See compiled SQL
cat target/compiled/claude_analytics/models/.../my_model.sql

# Check for errors in logs
cat logs/dbt.log

# Validate YAML syntax
dbt parse
```