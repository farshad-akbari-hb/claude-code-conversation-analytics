# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
npm install          # Install dependencies
npm run build        # Compile TypeScript to dist/
npm run dev          # Run with hot reload (tsx watch)
npm start            # Run compiled version

# Production (PM2)
pm2 start ecosystem.config.js
pm2 logs claude-mongo-sync
```

## Architecture

This service syncs Claude Code's JSONL conversation logs to MongoDB with local SQLite buffering for resilience.

### Data Flow

```
~/.claude/projects/**/*.jsonl → Watcher → SQLite Buffer → MongoDB
```

### Core Components

| File | Class | Responsibility |
|------|-------|----------------|
| `src/watcher.ts` | `Watcher` | Monitors JSONL files via chokidar, parses new lines, sends to buffer |
| `src/buffer.ts` | `Buffer` | SQLite persistence layer - stores file positions and pending entries |
| `src/sync.ts` | `MongoSync` | Periodic batch sync from SQLite to MongoDB |
| `src/index.ts` | - | Bootstrap, config loading, health endpoint, graceful shutdown |
| `src/types.ts` | - | TypeScript interfaces for entries, documents, stats |

### Resilience Pattern

The SQLite buffer (`~/.claude-sync/buffer.db`) provides durability:
- **file_positions** table: Tracks byte offset per JSONL file (enables incremental reads)
- **pending_entries** table: Queue of entries awaiting MongoDB sync

When MongoDB is unavailable, entries accumulate in SQLite. The `MongoSync.sync()` method handles reconnection and batch writes with duplicate key handling.

### Key Design Decisions

- **WAL mode SQLite**: Better concurrent read/write performance
- **Prepared statements**: All SQL queries pre-compiled at startup (`Buffer.prepareStatements()`)
- **Processing lock**: `Watcher.processing` Set prevents concurrent processing of same file
- **ordered: false**: MongoDB insertMany uses unordered for partial success on duplicates

## Configuration

Environment variables (see `src/index.ts` config object):

| Variable | Default | Purpose |
|----------|---------|---------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection |
| `MONGO_DB` | `claude_logs` | Database name |
| `CLAUDE_DIR` | `~/.claude/projects` | Watch directory |
| `SQLITE_PATH` | `~/.claude-sync/buffer.db` | Buffer location |
| `SYNC_INTERVAL_MS` | `5000` | Sync frequency |
| `BATCH_SIZE` | `100` | Entries per sync |
| `HEALTH_PORT` | `9090` | Health endpoint |

## Monitoring

```bash
# Health check
curl localhost:9090/health

# Buffer inspection
sqlite3 ~/.claude-sync/buffer.db "SELECT COUNT(*) FROM pending_entries WHERE synced=0"
```
