# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Sync Service (TypeScript)
npm install          # Install dependencies
npm run build        # Compile TypeScript to dist/
npm run dev          # Run with hot reload (tsx watch)
npm start            # Run compiled version
npm run lint         # Run ESLint

# UI (Next.js on port 3000)
cd ui && npm install # First time only
npm run dev:ui       # Development
npm run build:ui     # Production build

# Analytics (Python + dbt)
cd analytics
make up              # Start all services (Prefect + Metabase)
make deploy          # Deploy flows to Prefect server
make run-backfill    # Initial full backfill
make run-adhoc       # Incremental run
make logs            # View worker logs

# Production (PM2)
pm2 start ecosystem.config.js
pm2 logs claude-mongo-sync
```

## Architecture

This project has three main components:

1. **Sync Service** - Real-time JSONL to MongoDB sync with SQLite buffering
2. **UI** - Next.js application for browsing conversation logs
3. **Analytics** - ELT platform with dbt transformations and Metabase dashboards

### Complete Data Flow

```
~/.claude/projects/**/*.jsonl
         │
         ▼
    ┌─────────┐
    │ Watcher │ (chokidar)
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ SQLite  │ (buffer.db)
    │ Buffer  │
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ MongoDB │ ◄───────────────────────┐
    └────┬────┘                         │
         │                              │
    ┌────┴────┐                    ┌────┴────┐
    │         │                    │         │
    ▼         ▼                    │         │
┌──────┐  ┌──────────┐             │         │
│  UI  │  │ Analytics│             │  Sync   │
│(3000)│  │ Extractor│             │ Service │
└──────┘  └────┬─────┘             └─────────┘
               │
               ▼
          ┌─────────┐
          │ DuckDB  │ ← dbt (Bronze→Silver→Gold)
          └────┬────┘
               │
               ▼
          ┌──────────┐
          │ Metabase │
          │  (3001)  │
          └──────────┘
```

### Sync Service Components

| File | Class | Responsibility |
|------|-------|----------------|
| `src/watcher.ts` | `Watcher` | Monitors JSONL files via chokidar, parses new lines, sends to buffer |
| `src/buffer.ts` | `Buffer` | SQLite persistence layer - stores file positions and pending entries |
| `src/sync.ts` | `MongoSync` | Periodic batch sync from SQLite to MongoDB |
| `src/index.ts` | - | Bootstrap, config loading, health endpoint, graceful shutdown |
| `src/types.ts` | - | TypeScript interfaces for entries, documents, stats |

### Analytics Components

| Path | Responsibility |
|------|----------------|
| `analytics/analytics/extractor.py` | MongoDB extraction to Parquet |
| `analytics/analytics/loader.py` | DuckDB loading |
| `analytics/analytics/cli.py` | CLI entry point |
| `analytics/analytics/flows/` | Prefect orchestration flows |
| `analytics/dbt/models/staging/` | Bronze layer (cleaned source) |
| `analytics/dbt/models/intermediate/` | Silver layer (enriched) |
| `analytics/dbt/models/marts/` | Gold layer (star schema for BI) |

### Key Design Decisions

- **WAL mode SQLite**: Better concurrent read/write performance
- **Prepared statements**: All SQL queries pre-compiled at startup (`Buffer.prepareStatements()`)
- **Processing lock**: `Watcher.processing` Set prevents concurrent processing of same file
- **ordered: false**: MongoDB insertMany uses unordered for partial success on duplicates
- **Medallion architecture**: dbt models follow Bronze→Silver→Gold pattern for analytics

## Configuration

### Sync Service

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

| Variable | Default | Purpose |
|----------|---------|---------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection |
| `MONGO_DB` | `claude_logs` | Database name |
| `CLAUDE_DIR` | `~/.claude/projects` | Watch directory |
| `SQLITE_PATH` | `~/.claude-sync/buffer.db` | Buffer location |
| `SYNC_INTERVAL_MS` | `5000` | Sync frequency |
| `BATCH_SIZE` | `100` | Entries per sync |
| `HEALTH_PORT` | `9090` | Health endpoint |

### Analytics

Copy `.env.analytics.example` to `.env.analytics` in the analytics directory.

| Variable | Default | Purpose |
|----------|---------|---------|
| `DUCKDB_PATH` | `/duckdb/analytics.db` | DuckDB file path |
| `DBT_TARGET` | `dev` | dbt profile target |

## Testing

```bash
# Analytics Python tests
cd analytics && pytest tests/

# Analytics dbt tests
cd analytics/dbt && dbt test

# Data quality validation
cd analytics && python -m analytics.cli validate
```

## Monitoring

```bash
# Sync health check
curl localhost:9090/health

# Buffer inspection
sqlite3 ~/.claude-sync/buffer.db "SELECT COUNT(*) FROM pending_entries WHERE synced=0"

# Analytics pipeline status
cd analytics && make status
```

## Ports

| Service | Port | Purpose |
|---------|------|---------|
| Sync Health | 9090 | Health endpoint |
| UI | 3000 | Next.js application |
| Prefect UI | 4200 | Pipeline orchestration |
| Metabase | 3001 | Dashboards & analytics |
| dbt Docs | 8080 | Data model documentation |
