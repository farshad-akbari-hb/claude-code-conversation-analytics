# Claude Code Conversation Analytics

A complete platform for syncing, browsing, and analyzing Claude Code conversation logs. Features real-time JSONL sync to MongoDB, a Next.js UI for browsing conversations, and a dbt-based analytics pipeline with Metabase dashboards.

## Components

| Component | Description | Tech Stack |
|-----------|-------------|------------|
| **Sync Service** | Real-time JSONL to MongoDB sync with SQLite buffering | TypeScript, chokidar, better-sqlite3 |
| **UI** | Web interface for browsing conversation logs | Next.js, React, shadcn/ui, TanStack Query |
| **Analytics** | ELT pipeline with dimensional modeling | Python, dbt, DuckDB, Prefect, Metabase |

### Storage Formats

The analytics pipeline supports two storage formats:

| Format | Description | Use Case |
|--------|-------------|----------|
| **Parquet** (default) | Columnar file format with date partitioning | Simple deployments, quick setup |
| **Apache Iceberg** | Table format with ACID transactions, schema evolution, time travel | Production workloads, data versioning |

## Architecture

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
┌──────┐  ┌──────────┐             │  Sync   │
│  UI  │  │ Analytics│             │ Service │
│(3000)│  │ Extractor│             └─────────┘
└──────┘  └────┬─────┘
               │
               ▼
     ┌─────────────────┐
     │ Parquet/Iceberg │
     └────────┬────────┘
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

## Quick Start

```bash
# 1. Clone and install
git clone <repo>
cd claude-code-conversation-analytics

# 2. Configure environment
cp .env.example .env
# Edit .env with your MongoDB URI

# 3. Start sync service
cd sync-service && npm install && npm run dev

# 4. Start UI (new terminal)
cd ui && npm install && npm run dev

# 5. Start analytics (optional, new terminal)
cd analytics && make up && make deploy && make run-backfill
```

## Ports

| Service | Port | URL |
|---------|------|-----|
| Sync Health | 9090 | http://localhost:9090/health |
| UI | 3000 | http://localhost:3000 |
| Metabase | 3001 | http://localhost:3001 |
| Prefect UI | 4200 | http://localhost:4200 |
| dbt Docs | 8080 | http://localhost:8080 |

## Installation

### Sync Service

```bash
cd sync-service
npm install
npm run build
```

### UI

```bash
cd ui
npm install
npm run build
```

### Analytics

```bash
cd analytics
# Uses Docker Compose - no local install needed
make up
```

## Running

### Development

```bash
# Sync service (from root)
npm run dev

# UI (from root)
npm run dev:ui

# Analytics
cd analytics && make up && make deploy
```

### Production (PM2)

```bash
# Install PM2 globally
npm install -g pm2

# Start sync service
cd sync-service
pm2 start ecosystem.config.js

# Useful commands
pm2 status                    # Check status
pm2 logs claude-mongo-sync    # View logs
pm2 restart claude-mongo-sync # Restart
pm2 stop claude-mongo-sync    # Stop

# Auto-start on system boot
pm2 startup
pm2 save
```

### Production (systemd)

```bash
# Copy service file
sudo cp claude-mongo-sync@.service /etc/systemd/system/

# Copy application
sudo mkdir -p /opt/claude-mongo-sync
sudo cp -r sync-service/dist sync-service/package.json sync-service/node_modules /opt/claude-mongo-sync/
sudo cp .env /opt/claude-mongo-sync/

# Enable and start (replace YOUR_USERNAME)
sudo systemctl daemon-reload
sudo systemctl enable claude-mongo-sync@YOUR_USERNAME
sudo systemctl start claude-mongo-sync@YOUR_USERNAME
```

## Analytics Pipeline

The analytics component uses a medallion architecture (Bronze → Silver → Gold) with dbt transformations.

### Commands

```bash
cd analytics

# Infrastructure
make up              # Start all services (Prefect + Metabase)
make down            # Stop all services
make logs            # View worker logs
make status          # Show deployment status

# Pipeline runs (Parquet - default)
make deploy          # Deploy flows to Prefect server
make run-backfill    # Initial full backfill
make run-adhoc       # Incremental run
make run-daily       # Daily full refresh
make pipeline        # Run directly (no Prefect)

# Pipeline runs (Iceberg)
python -m analytics.cli extract --iceberg --full-backfill   # Extract to Iceberg
python -m analytics.cli load --iceberg                       # Load from Iceberg
python -m analytics.cli pipeline --iceberg                   # Full pipeline with Iceberg

# Iceberg table management
python -m analytics.cli iceberg info       # Show table info
python -m analytics.cli iceberg snapshots  # List snapshots (time travel)
python -m analytics.cli iceberg create     # Create table
python -m analytics.cli iceberg drop       # Drop table
```

## Monitoring

### Health Endpoint

```bash
curl http://localhost:9090/health
```

Response:
```json
{
  "status": "ok",
  "pending": 0,
  "synced": 1523,
  "lastSyncAt": "2024-12-15T10:30:00.000Z",
  "mongoConnected": true,
  "uptime": 3600
}
```

### SQLite Buffer Inspection

```bash
# Check pending entries
sqlite3 ~/.claude-sync/buffer.db "SELECT COUNT(*) FROM pending_entries WHERE synced=0"

# View recent entries
sqlite3 ~/.claude-sync/buffer.db "SELECT project_id, created_at FROM pending_entries ORDER BY id DESC LIMIT 10"

# Check file positions
sqlite3 ~/.claude-sync/buffer.db "SELECT * FROM file_positions"
```

### MongoDB Queries

```javascript
// Recent entries
db.conversations.find().sort({ ingestedAt: -1 }).limit(10)

// By project
db.conversations.find({ projectId: "abc123" })

// Count by project
db.conversations.aggregate([
  { $group: { _id: "$projectId", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])
```

## Configuration

### Sync Service

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `claude_logs` | Database name |
| `CLAUDE_DIR` | `~/.claude/projects` | Claude Code projects directory |
| `SQLITE_PATH` | `~/.claude-sync/buffer.db` | SQLite buffer location |
| `SYNC_INTERVAL_MS` | `5000` | Sync frequency (ms) |
| `BATCH_SIZE` | `100` | Entries per sync batch |
| `HEALTH_PORT` | `9090` | Health endpoint port |

### Analytics

| Variable | Default | Description |
|----------|---------|-------------|
| `DUCKDB_PATH` | `/duckdb/analytics.db` | DuckDB file path |
| `DBT_TARGET` | `dev` | dbt profile target |

### Iceberg (Optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `ICEBERG_WAREHOUSE_PATH` | `/data/iceberg` | Iceberg warehouse directory |
| `ICEBERG_CATALOG_NAME` | `default` | Catalog name |
| `ICEBERG_NAMESPACE` | `analytics` | Iceberg namespace |
| `ICEBERG_TABLE_NAME` | `conversations` | Table name |

## Resilience

| Failure | Behavior |
|---------|----------|
| MongoDB down | Entries buffered in SQLite, synced when available |
| Process restart | File positions restored from SQLite |
| Duplicate entries | Handled via MongoDB duplicate key errors |
| Partial sync failure | Unsynced entries remain pending |

## License

MIT
