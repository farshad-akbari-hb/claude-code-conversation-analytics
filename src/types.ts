export interface ClaudeEntry {
  type: string;
  sessionId?: string;
  timestamp?: string;
  message?: unknown;
  [key: string]: unknown;
}

export interface BufferedEntry {
  id: number;
  project_id: string;
  session_id: string | null;
  source_file: string;
  entry_json: string;
  created_at: string;
  synced: number;
}

export interface FilePosition {
  file_path: string;
  position: number;
  updated_at: string;
}

export interface MongoDocument extends ClaudeEntry {
  projectId: string;
  sourceFile: string;
  ingestedAt: Date;
}

export interface SyncStats {
  pending: number;
  synced: number;
  lastSyncAt: Date | null;
  mongoConnected: boolean;
}
