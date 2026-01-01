import { parseISO, startOfWeek, endOfWeek, format } from 'date-fns';
import { TimeGranularity } from './types';

export interface DateRange {
  startDate: string;
  endDate: string;
}

/**
 * Converts a periodStart value to a date range based on granularity.
 *
 * @param periodStart - The start of the period (format varies by granularity)
 * @param granularity - hour | day | week
 * @returns DateRange with startDate and endDate in YYYY-MM-DD format
 */
export function periodToDateRange(
  periodStart: string,
  granularity: TimeGranularity
): DateRange {
  switch (granularity) {
    case 'hour': {
      // periodStart: "2024-01-15T10:00" → filter that day
      const date = parseISO(periodStart);
      const dayStr = format(date, 'yyyy-MM-dd');
      return { startDate: dayStr, endDate: dayStr };
    }
    case 'day': {
      // periodStart: "2024-01-15" → filter that day
      return { startDate: periodStart, endDate: periodStart };
    }
    case 'week': {
      // periodStart: "2024-W03" → calculate week start/end
      return parseISOWeekToDateRange(periodStart);
    }
  }
}

/**
 * Parses ISO week format "YYYY-Www" to start/end dates.
 * ISO weeks start on Monday.
 */
function parseISOWeekToDateRange(isoWeek: string): DateRange {
  const match = isoWeek.match(/^(\d{4})-W(\d{2})$/);
  if (!match) {
    throw new Error(`Invalid ISO week format: ${isoWeek}`);
  }

  const [, yearStr, weekStr] = match;
  const year = parseInt(yearStr);
  const week = parseInt(weekStr);

  // Find the first Thursday of the year (ISO week 1 contains first Thursday)
  const jan4 = new Date(year, 0, 4);
  const firstMonday = startOfWeek(jan4, { weekStartsOn: 1 });

  // Calculate the Monday of the target week
  const weekStart = new Date(firstMonday);
  weekStart.setDate(weekStart.getDate() + (week - 1) * 7);

  const weekEnd = endOfWeek(weekStart, { weekStartsOn: 1 });

  return {
    startDate: format(weekStart, 'yyyy-MM-dd'),
    endDate: format(weekEnd, 'yyyy-MM-dd'),
  };
}
