'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

interface FilterPanelProps {
  onExport: () => void;
}

export function FilterPanel({ onExport }: FilterPanelProps) {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [projects, setProjects] = useState<string[]>([]);
  const [sessions, setSessions] = useState<string[]>([]);
  const [search, setSearch] = useState(searchParams.get('search') || '');

  const projectId = searchParams.get('projectId') || '';
  const sessionId = searchParams.get('sessionId') || '';
  const startDate = searchParams.get('startDate') || '';
  const endDate = searchParams.get('endDate') || '';

  // Fetch projects on mount
  useEffect(() => {
    fetch('/api/projects')
      .then((res) => res.json())
      .then((data) => setProjects(data.projects || []))
      .catch(console.error);
  }, []);

  // Fetch sessions when project changes
  useEffect(() => {
    if (projectId) {
      fetch(`/api/sessions?projectId=${encodeURIComponent(projectId)}`)
        .then((res) => res.json())
        .then((data) => setSessions(data.sessions || []))
        .catch(console.error);
    } else {
      setSessions([]);
    }
  }, [projectId]);

  const updateFilter = (key: string, value: string) => {
    const params = new URLSearchParams(searchParams.toString());
    if (value) {
      params.set(key, value);
    } else {
      params.delete(key);
    }
    // Reset session when project changes
    if (key === 'projectId') {
      params.delete('sessionId');
    }
    router.push(`?${params.toString()}`);
  };

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => {
      updateFilter('search', search);
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  return (
    <div className="bg-gray-50 p-4 rounded-lg mb-4">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
        {/* Project Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Project *
          </label>
          <select
            value={projectId}
            onChange={(e) => updateFilter('projectId', e.target.value)}
            className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 p-2 border"
          >
            <option value="">Select a project</option>
            {projects.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>

        {/* Session Filter */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Session
          </label>
          <select
            value={sessionId}
            onChange={(e) => updateFilter('sessionId', e.target.value)}
            disabled={!projectId}
            className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 p-2 border disabled:bg-gray-100"
          >
            <option value="">All sessions</option>
            {sessions.map((s) => (
              <option key={s} value={s}>
                {s.slice(0, 8)}...
              </option>
            ))}
          </select>
        </div>

        {/* Start Date */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Start Date
          </label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => updateFilter('startDate', e.target.value)}
            className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 p-2 border"
          />
        </div>

        {/* End Date */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            End Date
          </label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => updateFilter('endDate', e.target.value)}
            className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 p-2 border"
          />
        </div>

        {/* Search */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Search
          </label>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search messages..."
            className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 p-2 border"
          />
        </div>

        {/* Export Button */}
        <div className="flex items-end">
          <button
            onClick={onExport}
            disabled={!projectId}
            className="w-full bg-blue-600 text-white rounded-md py-2 px-4 hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            Export JSON
          </button>
        </div>
      </div>
    </div>
  );
}
