'use client';

import { useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import { FilterPanel } from '@/components/FilterPanel';
import { ConversationList } from '@/components/ConversationList';
import { useConversations } from '@/hooks/useConversations';
import { Conversation } from '@/lib/types';

function ConversationViewer() {
  const searchParams = useSearchParams();

  const projectId = searchParams.get('projectId') || '';
  const sessionId = searchParams.get('sessionId') || undefined;
  const search = searchParams.get('search') || undefined;
  const startDate = searchParams.get('startDate') || undefined;
  const endDate = searchParams.get('endDate') || undefined;

  const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading } =
    useConversations({
      projectId,
      sessionId,
      search,
      startDate,
      endDate,
    });

  const handleExport = () => {
    const params = new URLSearchParams();
    params.set('projectId', projectId);
    if (sessionId) params.set('sessionId', sessionId);
    if (search) params.set('search', search);
    if (startDate) params.set('startDate', startDate);
    if (endDate) params.set('endDate', endDate);

    window.open(`/api/conversations/export?${params.toString()}`, '_blank');
  };

  const conversations: Conversation[] =
    data?.pages.flatMap((page) => page.data) ?? [];
  const total = data?.pages[0]?.pagination.total ?? 0;

  return (
    <>
      <FilterPanel onExport={handleExport} />

      {isLoading && projectId ? (
        <div className="text-center py-12 text-gray-500">Loading...</div>
      ) : (
        <ConversationList
          conversations={conversations}
          hasMore={hasNextPage ?? false}
          total={total}
          onLoadMore={() => fetchNextPage()}
          isLoading={isFetchingNextPage}
        />
      )}
    </>
  );
}

export default function Home() {
  return (
    <main className="container mx-auto px-4 py-8">
      <header className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">
          Claude Conversation Viewer
        </h1>
        <p className="text-gray-600 mt-1">
          Browse and search Claude Code conversation logs stored in MongoDB
        </p>
      </header>

      <Suspense fallback={<div>Loading...</div>}>
        <ConversationViewer />
      </Suspense>
    </main>
  );
}
