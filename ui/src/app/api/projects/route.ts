import { NextResponse } from 'next/server';
import { getConversationsCollection } from '@/lib/mongodb';

export async function GET() {
  try {
    const collection = await getConversationsCollection();
    const projects = await collection.distinct('projectId');

    return NextResponse.json({ projects: projects.sort() });
  } catch (error) {
    console.error('Failed to fetch projects:', error);
    return NextResponse.json(
      { error: 'Failed to fetch projects' },
      { status: 500 }
    );
  }
}
