import { MongoClient, Collection, Db } from 'mongodb';
import { Buffer } from './buffer';
import { MongoDocument, SyncStats } from './types';

export class MongoSync {
  private client: MongoClient | null = null;
  private collection: Collection<MongoDocument> | null = null;
  private buffer: Buffer;
  private uri: string;
  private dbName: string;
  private batchSize: number;
  private syncInterval: number;
  private intervalId: NodeJS.Timeout | null = null;
  private lastSyncAt: Date | null = null;

  constructor(
    buffer: Buffer,
    options: {
      uri: string;
      dbName: string;
      batchSize?: number;
      syncIntervalMs?: number;
    }
  ) {
    this.buffer = buffer;
    this.uri = options.uri;
    this.dbName = options.dbName;
    this.batchSize = options.batchSize || 100;
    this.syncInterval = options.syncIntervalMs || 5000;
  }

  async connect(): Promise<boolean> {
    try {
      if (this.client) return true;

      this.client = await MongoClient.connect(this.uri, {
        serverSelectionTimeoutMS: 5000,
        connectTimeoutMS: 10000,
      });

      const db: Db = this.client.db(this.dbName);
      this.collection = db.collection<MongoDocument>('conversations');

      await this.collection.createIndex({ projectId: 1, timestamp: -1 });
      await this.collection.createIndex({ sessionId: 1 });
      await this.collection.createIndex({ 'ingestedAt': -1 });

      console.log('MongoDB connected');
      return true;
    } catch (err) {
      console.error('MongoDB connection failed:', (err as Error).message);
      this.client = null;
      this.collection = null;
      return false;
    }
  }

  async sync(): Promise<number> {
    const connected = await this.connect();
    if (!connected || !this.collection) {
      const stats = this.buffer.getStats();
      if (stats.pending > 0) {
        console.log(`MongoDB unavailable, ${stats.pending} entries buffered`);
      }
      return 0;
    }

    const pending = this.buffer.getPendingEntries(this.batchSize);
    if (pending.length === 0) return 0;

    const docs: MongoDocument[] = pending.map((row) => {
      const entry = JSON.parse(row.entry_json);
      return {
        ...entry,
        projectId: row.project_id,
        sourceFile: row.source_file,
        ingestedAt: new Date(),
      };
    });

    try {
      await this.collection.insertMany(docs, { ordered: false });
      this.buffer.markAsSynced(pending.map((r) => r.id));
      this.lastSyncAt = new Date();
      console.log(`Synced ${docs.length} entries to MongoDB`);
      return docs.length;
    } catch (err: unknown) {
      const mongoErr = err as { code?: number; writeErrors?: Array<{ code: number }> };
      
      // Handle duplicate key errors (partial success)
      if (mongoErr.code === 11000 || mongoErr.writeErrors?.some(e => e.code === 11000)) {
        this.buffer.markAsSynced(pending.map((r) => r.id));
        console.log('Handled duplicates, marked as synced');
        return docs.length;
      }

      console.error('Sync error:', (err as Error).message);
      await this.disconnect();
      return 0;
    }
  }

  startPeriodicSync(): void {
    // Initial sync
    this.sync();

    // Periodic sync
    this.intervalId = setInterval(() => this.sync(), this.syncInterval);
    console.log(`Sync interval: ${this.syncInterval}ms`);
  }

  stopPeriodicSync(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
      this.collection = null;
    }
  }

  getStats(): SyncStats {
    const bufferStats = this.buffer.getStats();
    return {
      pending: bufferStats.pending,
      synced: bufferStats.synced,
      lastSyncAt: this.lastSyncAt,
      mongoConnected: this.client !== null,
    };
  }
}
