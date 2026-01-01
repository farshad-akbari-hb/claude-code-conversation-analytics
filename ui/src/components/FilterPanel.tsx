'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { format } from 'date-fns';
import { CalendarIcon, Download, Search } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Calendar } from '@/components/ui/calendar';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from '@/components/ui/popover';
import { Card, CardContent } from '@/components/ui/card';
import { cn } from '@/lib/utils';

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

  // Extract display name from project path (last segment)
  const getProjectDisplayName = (path: string) => {
    const segments = path.split('-');
    // Return last 2-3 meaningful segments
    return segments.slice(-3).join('-');
  };

  return (
    <Card className="mb-6">
      <CardContent className="pt-6 space-y-4">
        {/* Row 1: Project & Session (wide fields) */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Project Filter */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Project *</label>
            <Select
              value={projectId}
              onValueChange={(value) => updateFilter('projectId', value)}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="Select project" />
              </SelectTrigger>
              <SelectContent>
                {projects.map((p) => (
                  <SelectItem key={p} value={p} className="max-w-[500px]">
                    <span className="truncate block" title={p}>
                      {getProjectDisplayName(p)}
                    </span>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {projectId && (
              <p className="text-xs text-muted-foreground truncate" title={projectId}>
                {projectId}
              </p>
            )}
          </div>

          {/* Session Filter */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Session</label>
            <Select
              value={sessionId || '__all__'}
              onValueChange={(value) => updateFilter('sessionId', value === '__all__' ? '' : value)}
              disabled={!projectId}
            >
              <SelectTrigger className="w-full">
                <SelectValue placeholder="All sessions" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__all__">All sessions</SelectItem>
                {sessions.map((s) => (
                  <SelectItem key={s} value={s}>
                    <code className="text-xs">{s}</code>
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            {sessionId && (
              <p className="text-xs text-muted-foreground font-mono">{sessionId}</p>
            )}
          </div>
        </div>

        {/* Row 2: Date Range, Search & Export */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
          {/* Start Date */}
          <div className="space-y-2">
            <label className="text-sm font-medium">Start Date</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="outline"
                  className={cn(
                    'w-full justify-start text-left font-normal',
                    !startDate && 'text-muted-foreground'
                  )}
                >
                  <CalendarIcon className="mr-2 h-4 w-4 shrink-0" />
                  <span className="truncate">
                    {startDate ? format(new Date(startDate), 'MMM d, yyyy') : 'Pick date'}
                  </span>
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar
                  mode="single"
                  selected={startDate ? new Date(startDate) : undefined}
                  onSelect={(date) =>
                    updateFilter('startDate', date ? format(date, 'yyyy-MM-dd') : '')
                  }
                  initialFocus
                />
              </PopoverContent>
            </Popover>
          </div>

          {/* End Date */}
          <div className="space-y-2">
            <label className="text-sm font-medium">End Date</label>
            <Popover>
              <PopoverTrigger asChild>
                <Button
                  variant="outline"
                  className={cn(
                    'w-full justify-start text-left font-normal',
                    !endDate && 'text-muted-foreground'
                  )}
                >
                  <CalendarIcon className="mr-2 h-4 w-4 shrink-0" />
                  <span className="truncate">
                    {endDate ? format(new Date(endDate), 'MMM d, yyyy') : 'Pick date'}
                  </span>
                </Button>
              </PopoverTrigger>
              <PopoverContent className="w-auto p-0" align="start">
                <Calendar
                  mode="single"
                  selected={endDate ? new Date(endDate) : undefined}
                  onSelect={(date) =>
                    updateFilter('endDate', date ? format(date, 'yyyy-MM-dd') : '')
                  }
                  initialFocus
                />
              </PopoverContent>
            </Popover>
          </div>

          {/* Search */}
          <div className="space-y-2 col-span-2 md:col-span-1 lg:col-span-2">
            <label className="text-sm font-medium">Search</label>
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search messages..."
                className="pl-8"
              />
            </div>
          </div>

          {/* Export Button */}
          <div className="space-y-2 col-span-2 md:col-span-1">
            <label className="text-sm font-medium invisible hidden md:block">Export</label>
            <Button
              onClick={onExport}
              disabled={!projectId}
              className="w-full"
            >
              <Download className="mr-2 h-4 w-4" />
              Export JSON
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
