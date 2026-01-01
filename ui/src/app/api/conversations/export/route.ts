import { NextRequest, NextResponse } from 'next/server';
import { Filter, Document } from 'mongodb';
import { z } from 'zod';
import { getConversationsCollection } from '@/lib/mongodb';

const querySchema = z.object({
  projectId: z.string().min(1),
  sessionId: z.string().optional(),
  search: z.string().optional(),
  startDate: z.string().optional(),
  endDate: z.string().optional(),
});

export async function GET(request: NextRequest) {
  const searchParams = Object.fromEntries(request.nextUrl.searchParams);
  const parsed = querySchema.safeParse(searchParams);

  if (!parsed.success) {
    return NextResponse.json(
      { error: 'Invalid parameters', details: parsed.error.issues },
      { status: 400 }
    );
  }

  const { projectId, sessionId, search, startDate, endDate } = parsed.data;

  try {
    const collection = await getConversationsCollection();

    // Build query
    const query: Filter<Document> = { projectId };

    if (sessionId) {
      query.sessionId = sessionId;
    }

    if (startDate || endDate) {
      query.ingestedAt = {};
      if (startDate) {
        query.ingestedAt.$gte = new Date(startDate);
      }
      if (endDate) {
        query.ingestedAt.$lte = new Date(endDate);
      }
    }

    if (search) {
      query.$text = { $search: search };
    }

    // Fetch all matching documents (with a reasonable limit)
    const docs = await collection
      .find(query)
      .sort({ ingestedAt: -1 })
      .limit(10000)
      .toArray();

    const filename = `conversations-${projectId}-${new Date().toISOString().split('T')[0]}.json`;

    return new NextResponse(JSON.stringify(docs, null, 2), {
      headers: {
        'Content-Type': 'application/json',
        'Content-Disposition': `attachment; filename="${filename}"`,
      },
    });
  } catch (error) {
    console.error('Failed to export conversations:', error);
    return NextResponse.json(
      { error: 'Failed to export conversations' },
      { status: 500 }
    );
  }
}
