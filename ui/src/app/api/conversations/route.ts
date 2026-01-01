import { NextRequest, NextResponse } from 'next/server';
import { ObjectId, Filter, Document } from 'mongodb';
import { z } from 'zod';
import { getConversationsCollection } from '@/lib/mongodb';

const querySchema = z.object({
  projectId: z.string().min(1),
  sessionId: z.string().optional(),
  search: z.string().optional(),
  startDate: z.string().optional(),
  endDate: z.string().optional(),
  cursor: z.string().optional(),
  limit: z.coerce.number().min(1).max(100).default(50),
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

  const { projectId, sessionId, search, startDate, endDate, cursor, limit } = parsed.data;

  try {
    const collection = await getConversationsCollection();

    // Build query
    const query: Filter<Document> = { projectId };

    if (sessionId) {
      query.sessionId = sessionId;
    }

    // Date range filter
    if (startDate || endDate) {
      query.ingestedAt = {};
      if (startDate) {
        query.ingestedAt.$gte = new Date(startDate);
      }
      if (endDate) {
        query.ingestedAt.$lte = new Date(endDate);
      }
    }

    // Full-text search
    if (search) {
      query.$text = { $search: search };
    }

    // Cursor-based pagination
    if (cursor) {
      try {
        const cursorId = new ObjectId(Buffer.from(cursor, 'base64').toString());
        query._id = { $lt: cursorId };
      } catch {
        return NextResponse.json({ error: 'Invalid cursor' }, { status: 400 });
      }
    }

    // Get total count (without cursor filter)
    const countQuery = { ...query };
    delete countQuery._id;
    const total = await collection.countDocuments(countQuery);

    // Fetch documents
    const docs = await collection
      .find(query)
      .sort({ ingestedAt: -1, _id: -1 })
      .limit(limit + 1)
      .toArray();

    const hasMore = docs.length > limit;
    const data = hasMore ? docs.slice(0, -1) : docs;
    const nextCursor = hasMore && data.length > 0
      ? Buffer.from(data[data.length - 1]._id.toString()).toString('base64')
      : null;

    return NextResponse.json({
      data,
      pagination: {
        nextCursor,
        hasMore,
        total,
      },
    });
  } catch (error) {
    console.error('Failed to fetch conversations:', error);
    return NextResponse.json(
      { error: 'Failed to fetch conversations' },
      { status: 500 }
    );
  }
}
