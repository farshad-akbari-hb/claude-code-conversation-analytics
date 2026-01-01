import Database, { Database as DatabaseType } from 'better-sqlite3';
import path from 'path';
import os from 'os';
import fs from 'fs';
import { BufferedEntry, ClaudeEntry } from './types';

export class Buffer {
  private db: DatabaseType;
  private statements: {
    getPosition: ReturnType<DatabaseType['prepare']>;
    upsertPosition: ReturnType<DatabaseType['prepare']>;
    insertEntry: ReturnType<DatabaseType['prepare']>;
    getPending: ReturnType<DatabaseType['prepare']>;
    markSynced: ReturnType<DatabaseType['prepare']>;
    cleanupSynced: ReturnType<DatabaseType['prepare']>;
    countPending: ReturnType<DatabaseType['prepare']>;
    countSynced: ReturnType<DatabaseType['prepare']>;
  };

  constructor(dbPath?: string) {
    const defaultPath = path.join(os.homedir(), '.claude-sync', 'buffer.db');
    const finalPath = dbPath || defaultPath;
    
    fs.mkdirSync(path.dirname(finalPath), { recursive: true });
    
    this.db = new Database(finalPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    
    this.initSchema();
    this.statements = this.prepareStatements();
  }

  private initSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS file_positions (
        file_path TEXT PRIMARY KEY,
        position INTEGER NOT NULL,
        updated_at TEXT NOT NULL
      );
      
      CREATE TABLE IF NOT EXISTS pending_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        project_id TEXT NOT NULL,
        session_id TEXT,
        source_file TEXT NOT NULL,
        entry_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        synced INTEGER DEFAULT 0
      );
      
      CREATE INDEX IF NOT EXISTS idx_pending_synced ON pending_entries(synced);
      CREATE INDEX IF NOT EXISTS idx_pending_project ON pending_entries(project_id);
    `);
  }

  private prepareStatements() {
    return {
      getPosition: this.db.prepare(
        'SELECT position FROM file_positions WHERE file_path = ?'
      ),
      upsertPosition: this.db.prepare(`
        INSERT INTO file_positions (file_path, position, updated_at) 
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(file_path) DO UPDATE SET position = excluded.position, updated_at = datetime('now')
      `),
      insertEntry: this.db.prepare(`
        INSERT INTO pending_entries (project_id, session_id, source_file, entry_json, created_at)
        VALUES (?, ?, ?, ?, datetime('now'))
      `),
      getPending: this.db.prepare(`
        SELECT id, entry_json, project_id, source_file, session_id 
        FROM pending_entries WHERE synced = 0 ORDER BY id LIMIT ?
      `),
      markSynced: this.db.prepare(
        'UPDATE pending_entries SET synced = 1 WHERE id = ?'
      ),
      cleanupSynced: this.db.prepare(`
        DELETE FROM pending_entries WHERE synced = 1 AND created_at < datetime('now', '-7 days')
      `),
      countPending: this.db.prepare(
        'SELECT COUNT(*) as count FROM pending_entries WHERE synced = 0'
      ),
      countSynced: this.db.prepare(
        'SELECT COUNT(*) as count FROM pending_entries WHERE synced = 1'
      ),
    };
  }

  getFilePosition(filePath: string): number {
    const row = this.statements.getPosition.get(filePath) as { position: number } | undefined;
    return row?.position || 0;
  }

  updateFilePosition(filePath: string, position: number): void {
    this.statements.upsertPosition.run(filePath, position);
  }

  insertEntries(entries: Array<{
    projectId: string;
    sessionId?: string;
    sourceFile: string;
    data: ClaudeEntry;
  }>): void {
    const insertMany = this.db.transaction((items: typeof entries) => {
      for (const entry of items) {
        this.statements.insertEntry.run(
          entry.projectId,
          entry.sessionId || null,
          entry.sourceFile,
          JSON.stringify(entry.data)
        );
      }
    });
    insertMany(entries);
  }

  getPendingEntries(limit: number): BufferedEntry[] {
    return this.statements.getPending.all(limit) as BufferedEntry[];
  }

  markAsSynced(ids: number[]): void {
    const markMany = this.db.transaction((idList: number[]) => {
      for (const id of idList) {
        this.statements.markSynced.run(id);
      }
    });
    markMany(ids);
  }

  cleanup(): void {
    this.statements.cleanupSynced.run();
  }

  getStats(): { pending: number; synced: number } {
    const pending = this.statements.countPending.get() as { count: number };
    const synced = this.statements.countSynced.get() as { count: number };
    return {
      pending: pending.count,
      synced: synced.count,
    };
  }

  close(): void {
    this.db.close();
  }
}
