'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import { Suspense } from 'react';
import { Loader2, MessageSquareText } from 'lucide-react';

import { FilterPanel } from '@/components/FilterPanel';
import { ConversationList } from '@/components/ConversationList';
import { useConversations } from '@/hooks/useConversations';
import { Conversation, SortOrder } from '@/lib/types';
import { Card, CardContent } from '@/components/ui/card';

function ConversationViewer() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const projectId = searchParams.get('projectId') || '';
  const sessionId = searchParams.get('sessionId') || undefined;
  const search = searchParams.get('search') || undefined;
  const startDate = searchParams.get('startDate') || undefined;
  const endDate = searchParams.get('endDate') || undefined;
  const sortOrder = (searchParams.get('sortOrder') as SortOrder) || 'desc';

  const { data, fetchNextPage, hasNextPage, isFetchingNextPage, isLoading } =
    useConversations({
      projectId,
      sessionId,
      search,
      startDate,
      endDate,
      sortOrder,
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

  const handleSortChange = (newSortOrder: SortOrder) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set('sortOrder', newSortOrder);
    router.push(`?${params.toString()}`);
  };

  const conversations: Conversation[] =
    data?.pages.flatMap((page) => page.data) ?? [];
  const total = data?.pages[0]?.pagination.total ?? 0;

  return (
    <>
      <FilterPanel onExport={handleExport} />

      {isLoading && projectId ? (
        <Card>
          <CardContent className="flex items-center justify-center py-16">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </CardContent>
        </Card>
      ) : (
        <ConversationList
          conversations={conversations}
          hasMore={hasNextPage ?? false}
          total={total}
          onLoadMore={() => fetchNextPage()}
          isLoading={isFetchingNextPage}
          sortOrder={sortOrder}
          onSortChange={handleSortChange}
        />
      )}
    </>
  );
}

export default function Home() {
  return (
    <main className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-8">
        <header className="mb-8">
          <div className="flex items-center gap-3 mb-2">
            <MessageSquareText className="h-8 w-8 text-primary" />
            <h1 className="text-3xl font-bold tracking-tight">
              Claude Conversation Viewer
            </h1>
          </div>
          <p className="text-muted-foreground">
            Browse and search Claude Code conversation logs stored in MongoDB
          </p>
        </header>

        <Suspense
          fallback={
            <Card>
              <CardContent className="flex items-center justify-center py-16">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
              </CardContent>
            </Card>
          }
        >
          <ConversationViewer />
        </Suspense>
      </div>
    </main>
  );
}
