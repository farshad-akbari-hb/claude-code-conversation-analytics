import { ObjectId } from 'mongodb';

export interface Conversation {
  _id: ObjectId;
  type: string;
  sessionId?: string;
  timestamp?: string;
  message?: unknown;
  projectId: string;
  sourceFile: string;
  ingestedAt: Date;
  [key: string]: unknown;
}

export interface ConversationsResponse {
  data: Conversation[];
  pagination: {
    nextCursor: string | null;
    hasMore: boolean;
    total: number;
  };
}

export interface ProjectsResponse {
  projects: string[];
}

export interface SessionsResponse {
  sessions: string[];
}

export interface FilterParams {
  projectId: string;
  sessionId?: string;
  search?: string;
  startDate?: string;
  endDate?: string;
  cursor?: string;
  limit?: number;
}
