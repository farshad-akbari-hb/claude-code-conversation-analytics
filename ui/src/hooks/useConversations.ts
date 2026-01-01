'use client';

import { useInfiniteQuery } from '@tanstack/react-query';
import { ConversationsResponse, FilterParams } from '@/lib/types';

async function fetchConversations(
  params: FilterParams & { cursor?: string }
): Promise<ConversationsResponse> {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value));
    }
  });

  const res = await fetch(`/api/conversations?${searchParams.toString()}`);
  if (!res.ok) {
    throw new Error('Failed to fetch conversations');
  }
  return res.json();
}

export function useConversations(filters: Omit<FilterParams, 'cursor' | 'limit'>) {
  return useInfiniteQuery({
    queryKey: ['conversations', filters],
    queryFn: ({ pageParam }) =>
      fetchConversations({ ...filters, cursor: pageParam, limit: 50 }),
    getNextPageParam: (lastPage) => lastPage.pagination.nextCursor,
    initialPageParam: undefined as string | undefined,
    enabled: !!filters.projectId,
  });
}
