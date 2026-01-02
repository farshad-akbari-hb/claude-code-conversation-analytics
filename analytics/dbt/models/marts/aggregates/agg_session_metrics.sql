-- Aggregate: Session Metrics Summary
-- Pre-computed session statistics for dashboard performance
-- Supports session productivity analysis

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'aggregate']
    )
}}

with sessions as (
    select * from {{ ref('dim_sessions') }}
),

-- Overall session statistics
session_stats as (
    select
        -- Counts
        count(*) as total_sessions,
        count(distinct project_id) as unique_projects,

        -- Duration metrics
        avg(duration_minutes) as avg_duration_minutes,
        percentile_cont(0.5) within group (order by duration_minutes) as median_duration_minutes,
        min(duration_minutes) as min_duration_minutes,
        max(duration_minutes) as max_duration_minutes,
        sum(duration_minutes) as total_duration_minutes,
        sum(duration_hours) as total_duration_hours,

        -- Message metrics
        avg(message_count) as avg_messages_per_session,
        sum(message_count) as total_messages,
        avg(user_message_count) as avg_user_messages,
        avg(assistant_message_count) as avg_assistant_messages,

        -- Tool metrics
        avg(tool_call_count) as avg_tools_per_session,
        sum(tool_call_count) as total_tool_calls,
        avg(unique_tools_used) as avg_unique_tools,

        -- Response time
        avg(avg_response_time_seconds) as overall_avg_response_time_seconds,

        -- Productivity metrics
        avg(messages_per_minute) as avg_messages_per_minute,
        avg(tools_per_minute) as avg_tools_per_minute

    from sessions
),

-- By duration category
by_duration_category as (
    select
        session_duration_category,
        count(*) as session_count,
        avg(duration_minutes) as avg_duration_minutes,
        avg(message_count) as avg_messages,
        avg(tool_call_count) as avg_tools
    from sessions
    group by session_duration_category
),

-- By activity level
by_activity_level as (
    select
        activity_level,
        count(*) as session_count,
        avg(duration_minutes) as avg_duration_minutes,
        avg(message_count) as avg_messages,
        avg(tool_call_count) as avg_tools
    from sessions
    group by activity_level
),

-- By time of day
by_time_of_day as (
    select
        session_time_of_day,
        count(*) as session_count,
        avg(duration_minutes) as avg_duration_minutes,
        avg(message_count) as avg_messages,
        avg(messages_per_minute) as avg_productivity
    from sessions
    group by session_time_of_day
),

-- By day type
by_day_type as (
    select
        session_day_type,
        count(*) as session_count,
        avg(duration_minutes) as avg_duration_minutes,
        avg(message_count) as avg_messages,
        sum(duration_hours) as total_hours
    from sessions
    group by session_day_type
),

-- Combine into single output
final as (
    select
        'overall' as metric_type,
        null as metric_key,
        ss.total_sessions,
        ss.unique_projects,
        ss.avg_duration_minutes,
        ss.median_duration_minutes,
        ss.total_duration_hours,
        ss.avg_messages_per_session,
        ss.total_messages,
        ss.avg_tools_per_session,
        ss.total_tool_calls,
        ss.overall_avg_response_time_seconds,
        ss.avg_messages_per_minute as productivity_score,
        current_timestamp as computed_at
    from session_stats ss

    union all

    select
        'by_duration_category' as metric_type,
        session_duration_category as metric_key,
        session_count as total_sessions,
        null as unique_projects,
        avg_duration_minutes,
        null as median_duration_minutes,
        null as total_duration_hours,
        avg_messages as avg_messages_per_session,
        null as total_messages,
        avg_tools as avg_tools_per_session,
        null as total_tool_calls,
        null as overall_avg_response_time_seconds,
        null as productivity_score,
        current_timestamp as computed_at
    from by_duration_category

    union all

    select
        'by_activity_level' as metric_type,
        activity_level as metric_key,
        session_count as total_sessions,
        null as unique_projects,
        avg_duration_minutes,
        null as median_duration_minutes,
        null as total_duration_hours,
        avg_messages as avg_messages_per_session,
        null as total_messages,
        avg_tools as avg_tools_per_session,
        null as total_tool_calls,
        null as overall_avg_response_time_seconds,
        null as productivity_score,
        current_timestamp as computed_at
    from by_activity_level

    union all

    select
        'by_time_of_day' as metric_type,
        session_time_of_day as metric_key,
        session_count as total_sessions,
        null as unique_projects,
        avg_duration_minutes,
        null as median_duration_minutes,
        null as total_duration_hours,
        avg_messages as avg_messages_per_session,
        null as total_messages,
        null as avg_tools_per_session,
        null as total_tool_calls,
        null as overall_avg_response_time_seconds,
        avg_productivity as productivity_score,
        current_timestamp as computed_at
    from by_time_of_day
)

select * from final
