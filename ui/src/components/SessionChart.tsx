'use client';

import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from 'recharts';
import { Loader2, BarChart3 } from 'lucide-react';

import {
  ChartContainer,
  ChartConfig,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
} from '@/components/ui/chart';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useStats } from '@/hooks/useStats';
import { TimeGranularity } from '@/lib/types';

const chartConfig: ChartConfig = {
  user: {
    label: 'User',
    color: 'hsl(221, 83%, 53%)',
  },
  assistant: {
    label: 'Assistant',
    color: 'hsl(142, 71%, 45%)',
  },
};

interface SessionChartProps {
  projectId: string;
  sessionId?: string;
  startDate?: string;
  endDate?: string;
}

export function SessionChart({
  projectId,
  sessionId,
  startDate,
  endDate,
}: SessionChartProps) {
  const [granularity, setGranularity] = useState<TimeGranularity>('day');
  const [periodCount, setPeriodCount] = useState(14);

  const { data, isLoading, isError } = useStats({
    projectId,
    sessionId,
    startDate,
    endDate,
    granularity,
    periodCount,
  });

  if (!projectId) return null;

  return (
    <Card className="mb-6">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base font-medium flex items-center gap-2">
            <BarChart3 className="h-4 w-4" />
            Message Activity
          </CardTitle>
          <ChartControls
            granularity={granularity}
            periodCount={periodCount}
            onGranularityChange={setGranularity}
            onPeriodCountChange={setPeriodCount}
          />
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <ChartLoadingState />
        ) : isError ? (
          <ChartErrorState />
        ) : !data?.data.length ? (
          <ChartEmptyState />
        ) : (
          <ChartContainer config={chartConfig} className="h-[250px] w-full">
            <BarChart accessibilityLayer data={data.data}>
              <CartesianGrid vertical={false} />
              <XAxis
                dataKey="period"
                tickLine={false}
                tickMargin={10}
                axisLine={false}
              />
              <YAxis tickLine={false} axisLine={false} />
              <ChartTooltip content={<ChartTooltipContent />} />
              <ChartLegend content={<ChartLegendContent />} />
              <Bar
                dataKey="user"
                stackId="messages"
                fill="var(--color-user)"
                radius={[0, 0, 0, 0]}
              />
              <Bar
                dataKey="assistant"
                stackId="messages"
                fill="var(--color-assistant)"
                radius={[4, 4, 0, 0]}
              />
            </BarChart>
          </ChartContainer>
        )}
      </CardContent>
    </Card>
  );
}

interface ChartControlsProps {
  granularity: TimeGranularity;
  periodCount: number;
  onGranularityChange: (value: TimeGranularity) => void;
  onPeriodCountChange: (value: number) => void;
}

function ChartControls({
  granularity,
  periodCount,
  onGranularityChange,
  onPeriodCountChange,
}: ChartControlsProps) {
  return (
    <div className="flex items-center gap-2">
      <Select
        value={granularity}
        onValueChange={(v) => onGranularityChange(v as TimeGranularity)}
      >
        <SelectTrigger className="w-[100px] h-8">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="hour">Hourly</SelectItem>
          <SelectItem value="day">Daily</SelectItem>
          <SelectItem value="week">Weekly</SelectItem>
        </SelectContent>
      </Select>

      <Select
        value={String(periodCount)}
        onValueChange={(v) => onPeriodCountChange(Number(v))}
      >
        <SelectTrigger className="w-[80px] h-8">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value="7">7</SelectItem>
          <SelectItem value="14">14</SelectItem>
          <SelectItem value="30">30</SelectItem>
          <SelectItem value="60">60</SelectItem>
        </SelectContent>
      </Select>
    </div>
  );
}

function ChartLoadingState() {
  return (
    <div className="flex items-center justify-center h-[250px]">
      <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
    </div>
  );
}

function ChartErrorState() {
  return (
    <div className="flex items-center justify-center h-[250px] text-muted-foreground">
      Failed to load chart data
    </div>
  );
}

function ChartEmptyState() {
  return (
    <div className="flex items-center justify-center h-[250px] text-muted-foreground">
      No data available for the selected period
    </div>
  );
}
