# Claude Analytics Platform

ELT analytics platform for Claude Code conversation logs. Extracts data from MongoDB, transforms using dbt (medallion architecture), and visualizes through Metabase.

## Architecture

```
MongoDB → Python Extractor → Parquet Files → DuckDB → dbt → Metabase
                                    ↑
                          Prefect Orchestrator (Hourly)
```

### Data Flow

1. **Extract**: Python pulls conversations from MongoDB, writes to Parquet
2. **Load**: Parquet files loaded into DuckDB
3. **Transform**: dbt models build Bronze → Silver → Gold layers
4. **Visualize**: Metabase dashboards query DuckDB

## Quick Start

### Prerequisites

- Docker & Docker Compose
- MongoDB running with conversation data

### Start the Stack

```bash
cd analytics

# Start all services
docker-compose -f docker-compose.analytics.yml up -d

# Check status
docker-compose -f docker-compose.analytics.yml ps
```

### Access Services

| Service | URL | Purpose |
|---------|-----|---------|
| Prefect UI | http://localhost:4200 | Pipeline orchestration |
| Metabase | http://localhost:3001 | Dashboards & analytics |
| dbt Docs | http://localhost:8080 | Data model documentation |

### Run Initial Backfill

```bash
# Trigger full backfill via Prefect
docker-compose -f docker-compose.analytics.yml exec analytics-worker \
  python -m analytics.cli pipeline --full-backfill --full-refresh
```

## Local Development

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Copy environment config
cp .env.analytics.example .env.analytics

# Edit .env.analytics with your MongoDB connection
```

### CLI Commands

```bash
# Show help
python -m analytics.cli --help

# View configuration
python -m analytics.cli config

# Run extraction
python -m analytics.cli extract --full-backfill

# Load into DuckDB
python -m analytics.cli load --stats

# Run dbt transformations
python -m analytics.cli transform

# Run complete pipeline
python -m analytics.cli pipeline

# Validate data quality
python -m analytics.cli validate
```

### dbt Development

```bash
cd dbt

# Install packages
dbt deps

# Run models
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

## Project Structure

```
analytics/
├── analytics/           # Python package
│   ├── __init__.py
│   ├── cli.py          # CLI entry point
│   ├── config.py       # Configuration management
│   ├── extractor.py    # MongoDB extraction
│   ├── loader.py       # DuckDB loading
│   ├── quality.py      # Great Expectations integration
│   └── flows/          # Prefect flows
│       ├── __init__.py
│       ├── main_pipeline.py
│       └── deployment.py
├── dbt/                 # dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/    # Bronze layer
│   │   ├── intermediate/ # Silver layer
│   │   └── marts/      # Gold layer (dimensions, facts, aggregates)
│   └── seeds/          # Reference data
├── great_expectations/  # Data quality
│   ├── great_expectations.yml
│   ├── expectations/
│   └── checkpoints/
├── metabase/           # Dashboard config
├── Dockerfile
├── docker-compose.analytics.yml
└── requirements.txt
```

## Data Model

### Medallion Architecture

| Layer | Schema | Purpose |
|-------|--------|---------|
| Bronze | `staging` | Cleaned source data |
| Silver | `intermediate` | Enriched & joined |
| Gold | `marts` | Star schema for BI |

### Key Tables

**Dimensions:**
- `dim_date` - Calendar dates with temporal attributes
- `dim_projects` - Projects with activity metrics
- `dim_sessions` - Sessions with duration and message counts
- `dim_tools` - Tool catalog with categories

**Facts:**
- `fct_messages` - Message-level analytics
- `fct_tool_calls` - Tool usage tracking
- `fct_file_operations` - Code activity

**Aggregates:**
- `agg_daily_summary` - Daily metrics
- `agg_hourly_activity` - Activity heatmap
- `agg_session_metrics` - Session statistics
- `agg_tool_efficiency` - Tool performance

## Configuration

Environment variables (see `.env.analytics.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection |
| `MONGO_DB` | `claude_logs` | Database name |
| `MONGO_COLLECTION` | `conversations` | Collection name |
| `DUCKDB_PATH` | `/duckdb/analytics.db` | DuckDB file path |
| `DBT_TARGET` | `dev` | dbt profile target |

## Operations

### Scheduled Pipeline

The hourly pipeline runs automatically via Prefect. To modify:

1. Edit schedule in `analytics/flows/deployment.py`
2. Apply changes: `python -m analytics.flows.deployment`

### Manual Runs

```bash
# Run incremental update
python -m analytics.cli pipeline

# Run with full refresh
python -m analytics.cli pipeline --full-refresh

# Run specific dbt models
python -m analytics.cli transform --models "+fct_messages"
```

### Data Quality

```bash
# Run all validations
python -m analytics.cli validate

# Run specific checkpoint
python -m analytics.cli validate --checkpoint analytics_checkpoint

# Build data docs
python -m analytics.cli validate --build-docs
```

### Troubleshooting

```bash
# Check pipeline status in Prefect
docker-compose -f docker-compose.analytics.yml logs prefect-server

# View worker logs
docker-compose -f docker-compose.analytics.yml logs analytics-worker

# Query DuckDB directly
docker-compose -f docker-compose.analytics.yml exec analytics-worker \
  python -c "import duckdb; print(duckdb.connect('/duckdb/analytics.db').execute('SELECT COUNT(*) FROM raw.conversations').fetchone())"
```

## Testing

```bash
# Run Python tests
pytest tests/

# Run dbt tests
cd dbt && dbt test

# Run data quality checks
python -m analytics.cli validate
```

## License

MIT
