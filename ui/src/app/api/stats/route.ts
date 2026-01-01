import { NextRequest, NextResponse } from 'next/server';
import { Filter, Document } from 'mongodb';
import { z } from 'zod';
import { getConversationsCollection } from '@/lib/mongodb';
import { format, parseISO } from 'date-fns';
import { ChartDataPoint, TimeGranularity } from '@/lib/types';

const querySchema = z.object({
  projectId: z.string().min(1),
  sessionId: z.string().optional(),
  startDate: z.string().optional(),
  endDate: z.string().optional(),
  granularity: z.enum(['hour', 'day', 'week']).default('day'),
  periodCount: z.coerce.number().min(1).max(90).default(14),
});

function getDateFormat(granularity: TimeGranularity): string {
  switch (granularity) {
    case 'hour':
      return '%Y-%m-%dT%H:00';
    case 'week':
      return '%G-W%V';
    case 'day':
    default:
      return '%Y-%m-%d';
  }
}

function formatPeriodLabel(period: string, granularity: TimeGranularity): string {
  try {
    if (granularity === 'week') {
      return period; // Already formatted as "2024-W01"
    }
    if (granularity === 'hour') {
      const date = parseISO(period);
      return format(date, 'MMM d HH:mm');
    }
    const date = parseISO(period);
    return format(date, 'MMM d');
  } catch {
    return period;
  }
}

interface AggregationResult {
  _id: {
    period: string;
    type: string;
  };
  count: number;
}

function transformToChartData(
  results: AggregationResult[],
  granularity: TimeGranularity,
  periodCount: number
): ChartDataPoint[] {
  const periodMap = new Map<string, { user: number; assistant: number }>();

  for (const result of results) {
    const period = result._id.period;
    const type = result._id.type;

    // Skip entries with null period (documents without valid timestamp)
    if (!period) continue;

    if (!periodMap.has(period)) {
      periodMap.set(period, { user: 0, assistant: 0 });
    }

    const entry = periodMap.get(period)!;
    if (type === 'user') {
      entry.user = result.count;
    } else if (type === 'assistant') {
      entry.assistant = result.count;
    }
  }

  return Array.from(periodMap.entries())
    .map(([period, counts]) => ({
      period: formatPeriodLabel(period, granularity),
      periodStart: period,
      ...counts,
    }))
    .sort((a, b) => a.periodStart.localeCompare(b.periodStart))
    .slice(-periodCount);
}

export async function GET(request: NextRequest) {
  const searchParams = Object.fromEntries(request.nextUrl.searchParams);
  const parsed = querySchema.safeParse(searchParams);

  if (!parsed.success) {
    return NextResponse.json(
      { error: 'Invalid parameters', details: parsed.error.issues },
      { status: 400 }
    );
  }

  const { projectId, sessionId, startDate, endDate, granularity, periodCount } = parsed.data;

  try {
    const collection = await getConversationsCollection();

    const matchStage: Filter<Document> = { projectId };

    if (sessionId) {
      matchStage.sessionId = sessionId;
    }

    // Build timestamp filter - always exclude null, optionally add date range
    const timestampFilter: Record<string, unknown> = {
      $ne: null,
      $exists: true,
    };
    if (startDate) {
      timestampFilter.$gte = startDate;
    }
    if (endDate) {
      timestampFilter.$lte = endDate + 'T23:59:59.999Z';
    }
    matchStage.timestamp = timestampFilter;

    const dateFormat = getDateFormat(granularity);

    const pipeline = [
      { $match: matchStage },
      {
        $group: {
          _id: {
            period: { $dateToString: { format: dateFormat, date: { $toDate: '$timestamp' } } },
            type: '$type',
          },
          count: { $sum: 1 },
        },
      },
      { $sort: { '_id.period': -1 as const } },
      { $limit: periodCount * 10 },
    ];

    const results = await collection.aggregate<AggregationResult>(pipeline).toArray();
    const chartData = transformToChartData(results, granularity, periodCount);

    return NextResponse.json({
      data: chartData,
      meta: {
        granularity,
        periodCount,
        totalMessages: chartData.reduce((sum, d) => sum + d.user + d.assistant, 0),
      },
    });
  } catch (error) {
    console.error('Failed to fetch stats:', error);
    return NextResponse.json(
      { error: 'Failed to fetch stats' },
      { status: 500 }
    );
  }
}
