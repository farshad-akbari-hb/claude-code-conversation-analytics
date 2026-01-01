'use client';

import { useState } from 'react';
import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from 'recharts';
import { Loader2, BarChart3, X } from 'lucide-react';

import {
  ChartContainer,
  ChartConfig,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
} from '@/components/ui/chart';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { useStats } from '@/hooks/useStats';
import { TimeGranularity, ChartDataPoint } from '@/lib/types';

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
  onBarClick?: (periodStart: string, granularity: TimeGranularity) => void;
  chartFilterActive?: boolean;
  onResetFilter?: () => void;
}

export function SessionChart({
  projectId,
  sessionId,
  startDate,
  endDate,
  onBarClick,
  chartFilterActive,
  onResetFilter,
}: SessionChartProps) {
  const [granularity, setGranularity] = useState<TimeGranularity>('day');
  const [periodCount, setPeriodCount] = useState(14);

  const handleBarClick = (data: { payload?: ChartDataPoint }) => {
    if (onBarClick && data.payload?.periodStart) {
      onBarClick(data.payload.periodStart, granularity);
    }
  };

  const { data: statsData, isLoading, isError } = useStats({
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
            {chartFilterActive && (
              <span className="text-xs bg-primary/10 text-primary px-2 py-0.5 rounded-full">
                Filtered
              </span>
            )}
          </CardTitle>
          <ChartControls
            granularity={granularity}
            periodCount={periodCount}
            onGranularityChange={setGranularity}
            onPeriodCountChange={setPeriodCount}
            chartFilterActive={chartFilterActive}
            onResetFilter={onResetFilter}
          />
        </div>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <ChartLoadingState />
        ) : isError ? (
          <ChartErrorState />
        ) : !statsData?.data.length ? (
          <ChartEmptyState />
        ) : (
          <ChartContainer config={chartConfig} className="h-[250px] w-full">
            <BarChart accessibilityLayer data={statsData.data}>
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
                onClick={handleBarClick}
                style={{ cursor: onBarClick ? 'pointer' : 'default' }}
              />
              <Bar
                dataKey="assistant"
                stackId="messages"
                fill="var(--color-assistant)"
                radius={[4, 4, 0, 0]}
                onClick={handleBarClick}
                style={{ cursor: onBarClick ? 'pointer' : 'default' }}
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
  chartFilterActive?: boolean;
  onResetFilter?: () => void;
}

function ChartControls({
  granularity,
  periodCount,
  onGranularityChange,
  onPeriodCountChange,
  chartFilterActive,
  onResetFilter,
}: ChartControlsProps) {
  return (
    <div className="flex items-center gap-2">
      {chartFilterActive && onResetFilter && (
        <Button
          variant="ghost"
          size="sm"
          onClick={onResetFilter}
          className="h-8 px-2 text-xs"
        >
          <X className="h-3 w-3 mr-1" />
          Clear filter
        </Button>
      )}
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
