# Claude Analytics Platform

ELT analytics platform for Claude Code conversation logs. Extracts from MongoDB, loads to DuckDB via Parquet, transforms with dbt, and visualizes through Metabase.

## Architecture

```
MongoDB (Source) → Python Extractor → Parquet Files → DuckDB → dbt → Metabase
                        ↑
                  Prefect Orchestrator (Batch: Hourly)
```

## Quick Start

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.analytics.example .env.analytics
# Edit .env.analytics with your MongoDB connection details

# Run CLI
python -m analytics.cli --help
```

### Docker

```bash
# Build image
docker build -t claude-analytics .

# Run with docker-compose (see docker-compose.analytics.yml)
docker-compose -f docker-compose.analytics.yml up
```

## CLI Commands

```bash
# Show configuration
python -m analytics.cli config

# Extract from MongoDB to Parquet
python -m analytics.cli extract --full-backfill

# Load Parquet to DuckDB
python -m analytics.cli load

# Run dbt transformations
python -m analytics.cli transform

# Run complete pipeline
python -m analytics.cli pipeline

# Validate data quality
python -m analytics.cli validate
```

## Project Structure

```
analytics/
├── analytics/           # Python package
│   ├── __init__.py
│   ├── cli.py          # CLI entry point
│   ├── config.py       # Configuration management
│   ├── extractor.py    # MongoDB extraction (Phase 2)
│   ├── loader.py       # DuckDB loading (Phase 3)
│   └── flows/          # Prefect flows (Phase 10)
├── dbt/                 # dbt project (Phase 4-9)
│   ├── models/
│   │   ├── staging/     # Bronze layer
│   │   ├── intermediate/# Silver layer
│   │   └── marts/       # Gold layer
│   └── seeds/
├── great_expectations/  # Data quality (Phase 12)
├── data/                # Parquet files (gitignored)
│   ├── raw/
│   ├── incremental/
│   └── dead_letter/
├── tests/               # Unit tests
├── Dockerfile
├── pyproject.toml
└── requirements.txt
```

## Configuration

Configuration is managed via environment variables. See `.env.analytics.example` for all options.

Key settings:
- `MONGO_URI`: MongoDB connection string
- `MONGO_DB`: Database name (default: `claude_logs`)
- `DUCKDB_PATH`: Path to DuckDB database file
- `BATCH_SIZE`: Documents per extraction batch

## Data Model

The platform uses a star schema with:
- **Dimensions**: `dim_date`, `dim_projects`, `dim_sessions`, `dim_tools`
- **Facts**: `fct_messages`, `fct_tool_calls`, `fct_file_operations`
- **Aggregates**: `agg_session_metrics`, `agg_daily_summary`, etc.

See `docs/SPEC.md` for the complete data model specification.
