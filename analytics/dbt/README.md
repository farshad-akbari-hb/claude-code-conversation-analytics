# Claude Analytics dbt Project

This dbt project transforms raw conversation data into analytics-ready models using a medallion architecture (Bronze → Silver → Gold).

## Architecture

```
raw.conversations (Source)
        ↓
    [Staging/Bronze]
    stg_conversations
    stg_messages
    stg_tool_calls
        ↓
    [Intermediate/Silver]
    int_messages_enriched
    int_sessions_computed
    int_tool_usage
        ↓
    [Marts/Gold]
    ┌─────────────┐     ┌─────────────┐
    │ Dimensions  │     │   Facts     │
    ├─────────────┤     ├─────────────┤
    │ dim_date    │     │fct_messages │
    │ dim_projects│     │fct_tool_calls│
    │ dim_sessions│     │fct_file_ops │
    │ dim_tools   │     └─────────────┘
    └─────────────┘
            ↓
        [Aggregates]
    agg_session_metrics
    agg_tool_efficiency
    agg_daily_summary
```

## Quick Start

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Commands

```bash
# Run specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run with full refresh (rebuild incremental models)
dbt run --full-refresh

# Run only specific model and its dependencies
dbt run --select +fct_messages

# Run seeds
dbt seed

# Run tests
dbt test
dbt test --select staging
```

## Configuration

The project uses environment variables for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PATH` | `/duckdb/analytics.db` | Path to DuckDB database |

## Model Layers

### Staging (Bronze)
- Direct transformations from source
- Minimal logic: type casting, renaming, null handling
- Materialized as views for quick iteration

### Intermediate (Silver)
- Business logic and enrichment
- Joins between staging models
- Classification and computed fields

### Marts (Gold)
- Star schema dimensions and facts
- Optimized for BI tools
- Materialized as tables for performance

### Aggregates
- Pre-computed metrics for dashboards
- Reduces query time for common analyses

## Seeds

| Seed | Purpose |
|------|---------|
| `tool_categories.csv` | Maps tool names to categories |

## Testing

Tests are defined in `schema.yml` files:
- Column uniqueness and not-null constraints
- Referential integrity between models
- Accepted values for categorical columns
