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

export type SortOrder = 'asc' | 'desc';

export interface FilterParams {
  projectId: string;
  sessionId?: string;
  search?: string;
  startDate?: string;
  endDate?: string;
  cursor?: string;
  limit?: number;
  sortOrder?: SortOrder;
}

// Chart types
export type TimeGranularity = 'hour' | 'day' | 'week';

export interface ChartDataPoint {
  period: string;
  periodStart: string;
  user: number;
  assistant: number;
}

export interface StatsResponse {
  data: ChartDataPoint[];
  meta: {
    granularity: TimeGranularity;
    periodCount: number;
    totalMessages: number;
  };
}

export interface StatsParams {
  projectId: string;
  sessionId?: string;
  startDate?: string;
  endDate?: string;
  granularity: TimeGranularity;
  periodCount: number;
}
