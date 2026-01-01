import { NextRequest, NextResponse } from 'next/server';
import { getConversationsCollection } from '@/lib/mongodb';

export async function GET(request: NextRequest) {
  const projectId = request.nextUrl.searchParams.get('projectId');

  if (!projectId) {
    return NextResponse.json(
      { error: 'projectId is required' },
      { status: 400 }
    );
  }

  try {
    const collection = await getConversationsCollection();
    const sessions = await collection.distinct('sessionId', { projectId });

    const validSessions = sessions
      .filter((s): s is string => s != null && s !== '')
      .sort();

    return NextResponse.json({ sessions: validSessions });
  } catch (error) {
    console.error('Failed to fetch sessions:', error);
    return NextResponse.json(
      { error: 'Failed to fetch sessions' },
      { status: 500 }
    );
  }
}
