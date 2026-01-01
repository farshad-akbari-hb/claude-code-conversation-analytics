'use client';

import { useQuery } from '@tanstack/react-query';
import { StatsResponse, StatsParams } from '@/lib/types';

async function fetchStats(params: StatsParams): Promise<StatsResponse> {
  const searchParams = new URLSearchParams();

  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== '') {
      searchParams.set(key, String(value));
    }
  });

  const res = await fetch(`/api/stats?${searchParams.toString()}`);
  if (!res.ok) {
    throw new Error('Failed to fetch stats');
  }
  return res.json();
}

export function useStats(options: StatsParams) {
  return useQuery({
    queryKey: ['stats', options],
    queryFn: () => fetchStats(options),
    enabled: !!options.projectId,
    staleTime: 60 * 1000,
  });
}
