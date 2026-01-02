# Metabase Configuration for Claude Analytics

This directory contains configuration and documentation for setting up Metabase dashboards.

## Quick Start

1. Start the analytics stack:
   ```bash
   docker-compose -f docker-compose.analytics.yml up -d
   ```

2. Access Metabase at http://localhost:3001

3. Complete the initial setup wizard

4. Add DuckDB as a data source (see below)

## Adding DuckDB Connection

Metabase supports DuckDB via the official community driver.

### Connection Settings

| Setting | Value |
|---------|-------|
| Database type | DuckDB |
| Display name | Claude Analytics |
| Database file path | `/duckdb/analytics.db` |

**Note**: The DuckDB file is mounted read-only in the Metabase container.

## Pre-built Dashboards

After connecting to DuckDB, create the following dashboards:

### 1. Developer Productivity Dashboard

**Questions to create:**

1. **Sessions Over Time**
   - Table: `marts.agg_daily_summary`
   - Visualization: Line chart
   - X-axis: `date_key`
   - Y-axis: `session_count`

2. **Average Session Duration**
   - Table: `marts.dim_sessions`
   - Visualization: Trend line
   - Metric: AVG of `duration_minutes`

3. **Messages per Session**
   - Table: `marts.agg_session_metrics`
   - Filter: `metric_type = 'overall'`
   - Metric: `avg_messages_per_session`

4. **Active Coding Hours Heatmap**
   - Table: `marts.agg_hourly_activity`
   - Visualization: Heatmap/Pivot
   - Rows: `day_name`
   - Columns: `hour_of_day`
   - Values: `total_activity`

### 2. AI Interaction Patterns Dashboard

**Questions to create:**

1. **Tool Usage Distribution**
   - Table: `marts.agg_tool_efficiency`
   - Visualization: Pie chart
   - Dimension: `tool_name`
   - Metric: `total_calls`

2. **Tool Usage by Category**
   - Table: `marts.agg_tool_efficiency`
   - Visualization: Bar chart
   - Dimension: `tool_category`
   - Metric: `total_calls`

3. **Tool Efficiency Ranking**
   - Table: `marts.agg_tool_efficiency`
   - Visualization: Row chart
   - Sorted by: `popularity_rank`
   - Show: top 10

4. **Average Tool Execution Time**
   - Table: `marts.agg_tool_efficiency`
   - Visualization: Bar chart
   - Dimension: `tool_name`
   - Metric: `avg_execution_seconds`

### 3. Project Insights Dashboard

**Questions to create:**

1. **Activity by Project**
   - Table: `marts.dim_projects`
   - Visualization: Bar chart
   - Dimension: `project_id`
   - Metric: `total_messages`

2. **Project Activity Status**
   - Table: `marts.dim_projects`
   - Visualization: Pie chart
   - Dimension: `activity_status`
   - Metric: Count

3. **Code Changes Over Time**
   - Table: `marts.agg_daily_summary`
   - Visualization: Line chart
   - X-axis: `date_key`
   - Y-axis: `file_operation_count`

4. **File Type Distribution**
   - Table: `marts.agg_code_changes`
   - Filter: `aggregation_level = 'by_file_type'`
   - Visualization: Pie chart
   - Dimension: `aggregation_key`
   - Metric: `total_operations`

## Sample SQL Queries

### Most Active Hours

```sql
SELECT
    hour_label,
    day_name,
    total_activity
FROM marts.agg_hourly_activity
ORDER BY total_activity DESC
LIMIT 10;
```

### Daily Session Trends (Last 30 Days)

```sql
SELECT
    date_key,
    session_count,
    message_count,
    tool_call_count
FROM marts.agg_daily_summary
WHERE date_key >= current_date - interval '30 days'
ORDER BY date_key;
```

### Top Tools by Usage

```sql
SELECT
    tool_name,
    tool_category,
    total_calls,
    sessions_used,
    popularity_rank
FROM marts.agg_tool_efficiency
ORDER BY popularity_rank
LIMIT 10;
```

### Task Category Distribution

```sql
SELECT
    task_category,
    COUNT(*) as message_count
FROM marts.fct_messages
GROUP BY task_category
ORDER BY message_count DESC;
```

## Filters

Add the following filters to dashboards for interactivity:

1. **Date Range Filter**
   - Field: `date_key`
   - Default: Last 30 days

2. **Project Filter**
   - Field: `project_id`
   - Type: Multi-select

3. **Activity Level Filter**
   - Field: `activity_level`
   - Values: minimal, light, moderate, heavy

## Refresh Schedule

Configure automatic question refresh:

- Real-time dashboards: 1 hour
- Historical analysis: 6 hours
- Aggregates: Daily at 3 AM (after pipeline runs)
