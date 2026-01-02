-- Aggregate: Hourly Activity Pattern
-- Pre-computed hourly metrics for activity heatmaps
-- Supports work pattern and productivity analysis

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'aggregate']
    )
}}

with messages as (
    select * from {{ ref('int_messages_enriched') }}
),

tool_calls as (
    select * from {{ ref('fct_tool_calls') }}
),

sessions as (
    select * from {{ ref('dim_sessions') }}
),

-- Hourly message activity
hourly_messages as (
    select
        hour_of_day,
        day_of_week,
        count(*) as message_count,
        count(distinct session_id) as unique_sessions,
        count(distinct project_id) as unique_projects,
        avg(content_length) as avg_content_length,
        sum(case when role = 'user' then 1 else 0 end) as user_messages,
        sum(case when role = 'assistant' then 1 else 0 end) as assistant_messages,
        sum(case when has_code_block then 1 else 0 end) as messages_with_code
    from messages
    group by hour_of_day, day_of_week
),

-- Hourly tool activity
hourly_tools as (
    select
        hour_of_day,
        day_of_week,
        count(*) as tool_call_count,
        count(distinct tool_name) as unique_tools,
        sum(case when is_file_operation then 1 else 0 end) as file_operations,
        sum(case when is_search_operation then 1 else 0 end) as search_operations,
        sum(case when is_shell_command then 1 else 0 end) as shell_commands
    from tool_calls
    group by hour_of_day, day_of_week
),

-- Session starts by hour
session_starts as (
    select
        start_hour as hour_of_day,
        start_day_of_week as day_of_week,
        count(*) as session_starts,
        avg(duration_minutes) as avg_session_duration
    from sessions
    group by start_hour, start_day_of_week
),

-- Create complete hour x day matrix
hour_day_matrix as (
    select
        h.hour_of_day,
        d.day_of_week
    from (select unnest(generate_series(0, 23)) as hour_of_day) h
    cross join (select unnest(generate_series(0, 6)) as day_of_week) d
),

-- Combine all metrics
hourly_activity as (
    select
        hdm.hour_of_day,
        hdm.day_of_week,

        -- Day labels
        case hdm.day_of_week
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,

        case
            when hdm.day_of_week in (0, 6) then 'weekend'
            else 'weekday'
        end as day_type,

        -- Time period labels
        case
            when hdm.hour_of_day between 6 and 11 then 'morning'
            when hdm.hour_of_day between 12 and 17 then 'afternoon'
            when hdm.hour_of_day between 18 and 21 then 'evening'
            else 'night'
        end as time_of_day,

        -- Hour label (for display)
        lpad(hdm.hour_of_day::text, 2, '0') || ':00' as hour_label,

        -- Message metrics
        coalesce(hm.message_count, 0) as message_count,
        coalesce(hm.unique_sessions, 0) as unique_sessions,
        coalesce(hm.unique_projects, 0) as unique_projects,
        round(coalesce(hm.avg_content_length, 0)::numeric, 0) as avg_content_length,
        coalesce(hm.user_messages, 0) as user_messages,
        coalesce(hm.assistant_messages, 0) as assistant_messages,
        coalesce(hm.messages_with_code, 0) as messages_with_code,

        -- Tool metrics
        coalesce(ht.tool_call_count, 0) as tool_call_count,
        coalesce(ht.unique_tools, 0) as unique_tools,
        coalesce(ht.file_operations, 0) as file_operations,
        coalesce(ht.search_operations, 0) as search_operations,
        coalesce(ht.shell_commands, 0) as shell_commands,

        -- Session metrics
        coalesce(ss.session_starts, 0) as session_starts,
        round(coalesce(ss.avg_session_duration, 0)::numeric, 2) as avg_session_duration,

        -- Combined activity score (for heatmap coloring)
        coalesce(hm.message_count, 0) + coalesce(ht.tool_call_count, 0) as total_activity,

        -- Metadata
        current_timestamp as computed_at

    from hour_day_matrix hdm
    left join hourly_messages hm on hdm.hour_of_day = hm.hour_of_day and hdm.day_of_week = hm.day_of_week
    left join hourly_tools ht on hdm.hour_of_day = ht.hour_of_day and hdm.day_of_week = ht.day_of_week
    left join session_starts ss on hdm.hour_of_day = ss.hour_of_day and hdm.day_of_week = ss.day_of_week
)

select * from hourly_activity
order by day_of_week, hour_of_day
