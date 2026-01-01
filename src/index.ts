import 'dotenv/config';
import path from 'path';
import os from 'os';
import http from 'http';
import { Buffer } from './buffer';
import { Watcher } from './watcher';
import { MongoSync } from './sync';

function expandTilde(filePath: string): string {
  if (filePath.startsWith('~/')) {
    return path.join(os.homedir(), filePath.slice(2));
  }
  return filePath;
}

// Configuration from environment
const config = {
  claudeDir: expandTilde(process.env.CLAUDE_DIR || path.join(os.homedir(), '.claude', 'projects')),
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017',
  dbName: process.env.MONGO_DB || 'claude_logs',
  sqlitePath: expandTilde(process.env.SQLITE_PATH || path.join(os.homedir(), '.claude-sync', 'buffer.db')),
  syncIntervalMs: parseInt(process.env.SYNC_INTERVAL_MS || '5000', 10),
  batchSize: parseInt(process.env.BATCH_SIZE || '100', 10),
  cleanupIntervalMs: parseInt(process.env.CLEANUP_INTERVAL_MS || '3600000', 10), // 1 hour
  healthPort: parseInt(process.env.HEALTH_PORT || '9090', 10),
};

let buffer: Buffer;
let watcher: Watcher;
let mongoSync: MongoSync;
let cleanupIntervalId: NodeJS.Timeout;
let healthServer: http.Server;

async function main(): Promise<void> {
  console.log('Starting Claude MongoDB Sync');
  console.log(`Watch dir: ${config.claudeDir}`);
  console.log(`Buffer: ${config.sqlitePath}`);
  console.log(`MongoDB: ${config.mongoUri}/${config.dbName}`);

  // Initialize components
  buffer = new Buffer(config.sqlitePath);
  
  watcher = new Watcher(config.claudeDir, buffer);
  watcher.start();

  mongoSync = new MongoSync(buffer, {
    uri: config.mongoUri,
    dbName: config.dbName,
    batchSize: config.batchSize,
    syncIntervalMs: config.syncIntervalMs,
  });
  mongoSync.startPeriodicSync();

  // Periodic cleanup of old synced entries
  cleanupIntervalId = setInterval(() => {
    buffer.cleanup();
    console.log('Cleanup completed');
  }, config.cleanupIntervalMs);

  // Health check endpoint for monitoring
  healthServer = http.createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      const stats = mongoSync.getStats();
      const response = {
        status: 'ok',
        ...stats,
        uptime: process.uptime(),
      };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(response, null, 2));
    } else if (req.url === '/stats') {
      const stats = mongoSync.getStats();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(stats, null, 2));
    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });

  healthServer.listen(config.healthPort, () => {
    console.log(`Health endpoint: http://localhost:${config.healthPort}/health`);
  });

  console.log('Ready');
}

async function shutdown(signal: string): Promise<void> {
  console.log(`\n${signal} received, shutting down gracefully...`);

  // Stop accepting new work
  watcher?.stop();
  mongoSync?.stopPeriodicSync();
  clearInterval(cleanupIntervalId);

  // Final sync attempt
  try {
    console.log('Final sync...');
    await mongoSync?.sync();
  } catch (e) {
    console.error('Final sync failed:', (e as Error).message);
  }

  // Cleanup
  await mongoSync?.disconnect();
  buffer?.close();
  healthServer?.close();

  console.log('Shutdown complete');
  process.exit(0);
}

// Graceful shutdown handlers
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  shutdown('uncaughtException');
});
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
});

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
