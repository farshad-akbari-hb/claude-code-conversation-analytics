# Claude MongoDB Sync

Real-time sync of Claude Code JSONL conversation logs to MongoDB with local SQLite buffering for resilience.

## Features

- **Real-time watching** of Claude Code JSONL files
- **SQLite buffer** survives MongoDB downtime and restarts
- **Batched writes** for efficient MongoDB operations
- **Health endpoint** for monitoring
- **Graceful shutdown** with final sync attempt
- **Auto-cleanup** of old synced entries

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
    │ Buffer  │ - file positions
    └────┬────┘ - pending entries
         │
         ▼
    ┌─────────┐
    │  Sync   │ (every 5s)
    │ Worker  │
    └────┬────┘
         │
         ▼
    ┌─────────┐
    │ MongoDB │
    └─────────┘
```

## Installation

```bash
# Clone and install
git clone <repo>
cd claude-mongo-sync
npm install

# Build TypeScript
npm run build

# Configure
cp .env.example .env
# Edit .env with your MongoDB URI
```

## Running

### Option 1: PM2 (Recommended)

```bash
# Install PM2 globally
npm install -g pm2

# Start service
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

### Option 2: systemd (Production Linux)

```bash
# Copy service file
sudo cp claude-mongo-sync@.service /etc/systemd/system/

# Copy application
sudo mkdir -p /opt/claude-mongo-sync
sudo cp -r dist package.json node_modules /opt/claude-mongo-sync/
sudo cp .env /opt/claude-mongo-sync/

# Enable and start (replace YOUR_USERNAME)
sudo systemctl daemon-reload
sudo systemctl enable claude-mongo-sync@YOUR_USERNAME
sudo systemctl start claude-mongo-sync@YOUR_USERNAME

# Check status
sudo systemctl status claude-mongo-sync@YOUR_USERNAME
journalctl -u claude-mongo-sync@YOUR_USERNAME -f
```

### Option 3: Development

```bash
npm run dev
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

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `claude_logs` | Database name |
| `CLAUDE_DIR` | `~/.claude/projects` | Claude Code projects directory |
| `SQLITE_PATH` | `~/.claude-sync/buffer.db` | SQLite buffer location |
| `SYNC_INTERVAL_MS` | `5000` | Sync frequency (ms) |
| `BATCH_SIZE` | `100` | Entries per sync batch |
| `HEALTH_PORT` | `9090` | Health endpoint port |

## Resilience

| Failure | Behavior |
|---------|----------|
| MongoDB down | Entries buffered in SQLite, synced when available |
| Process restart | File positions restored from SQLite |
| Duplicate entries | Handled via MongoDB duplicate key errors |
| Partial sync failure | Unsynced entries remain pending |

## License

MIT
