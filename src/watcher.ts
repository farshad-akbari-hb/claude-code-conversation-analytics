import chokidar, { FSWatcher } from 'chokidar';
import fs from 'fs';
import readline from 'readline';
import path from 'path';
import { Buffer } from './buffer';
import { ClaudeEntry } from './types';

export class Watcher {
  private watcher: FSWatcher | null = null;
  private buffer: Buffer;
  private watchDir: string;
  private processing = new Set<string>();

  constructor(watchDir: string, buffer: Buffer) {
    this.watchDir = watchDir;
    this.buffer = buffer;
  }

  start(): void {
    this.watcher = chokidar.watch(`${this.watchDir}/**/*.jsonl`, {
      persistent: true,
      ignoreInitial: false,
      awaitWriteFinish: {
        stabilityThreshold: 300,
        pollInterval: 100,
      },
      usePolling: false,
      alwaysStat: true,
    });

    this.watcher.on('add', (filePath) => this.processFile(filePath));
    this.watcher.on('change', (filePath) => this.processFile(filePath));
    this.watcher.on('error', (error) => console.error('Watcher error:', error));

    console.log(`Watching: ${this.watchDir}`);
  }

  private async processFile(filePath: string): Promise<void> {
    // Prevent concurrent processing of same file
    if (this.processing.has(filePath)) return;
    this.processing.add(filePath);

    try {
      const projectId = this.extractProjectId(filePath);
      const startPos = this.buffer.getFilePosition(filePath);

      let stats: fs.Stats;
      try {
        stats = fs.statSync(filePath);
      } catch {
        return; // File deleted
      }

      if (stats.size <= startPos) return;

      const stream = fs.createReadStream(filePath, {
        start: startPos,
        encoding: 'utf8',
      });

      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
      });

      const entries: Array<{
        projectId: string;
        sessionId?: string;
        sourceFile: string;
        data: ClaudeEntry;
      }> = [];

      for await (const line of rl) {
        if (!line.trim()) continue;
        try {
          const data = JSON.parse(line) as ClaudeEntry;
          entries.push({
            projectId,
            sessionId: data.sessionId,
            sourceFile: filePath,
            data,
          });
        } catch (e) {
          console.error(`Parse error in ${path.basename(filePath)}:`, (e as Error).message);
        }
      }

      if (entries.length > 0) {
        this.buffer.insertEntries(entries);
        console.log(`Buffered ${entries.length} entries from ${path.basename(filePath)}`);
      }

      this.buffer.updateFilePosition(filePath, stats.size);
    } finally {
      this.processing.delete(filePath);
    }
  }

  private extractProjectId(filePath: string): string {
    const match = filePath.match(/projects\/([^/]+)/);
    return match ? match[1] : 'unknown';
  }

  async stop(): Promise<void> {
    if (this.watcher) {
      await this.watcher.close();
      this.watcher = null;
    }
  }
}
