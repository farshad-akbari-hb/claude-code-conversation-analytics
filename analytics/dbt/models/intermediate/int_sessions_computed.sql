-- Intermediate model for computed session metrics
-- Aggregates session-level statistics
-- Silver layer with session analytics

{{
    config(
        materialized='view',
        tags=['intermediate', 'silver']
    )
}}

with conversations as (
    select * from {{ ref('stg_conversations') }}
),

messages as (
    select * from {{ ref('stg_messages') }}
),

tool_calls as (
    select * from {{ ref('stg_tool_calls') }}
),

-- Session boundaries from all entries
session_boundaries as (
    select
        session_id,
        project_id,
        min(effective_timestamp) as session_start,
        max(effective_timestamp) as session_end,
        min(partition_date) as start_date,
        max(partition_date) as end_date,
        count(*) as total_entries
    from conversations
    where session_id is not null
    group by session_id, project_id
),

-- Message counts per session
message_stats as (
    select
        session_id,
        count(*) as message_count,
        sum(case when role = 'user' then 1 else 0 end) as user_message_count,
        sum(case when role = 'assistant' then 1 else 0 end) as assistant_message_count,
        sum(content_length) as total_content_length,
        avg(content_length) as avg_content_length,
        avg(seconds_since_previous) filter (where seconds_since_previous > 0) as avg_response_time_seconds
    from messages
    group by session_id
),

-- Tool usage per session
tool_stats as (
    select
        session_id,
        count(*) as tool_call_count,
        count(distinct tool_name) as unique_tools_used,
        sum(case when is_invocation then 1 else 0 end) as tool_invocations,
        sum(case when is_result then 1 else 0 end) as tool_results,
        -- Most used tool
        mode() within group (order by tool_name) as primary_tool
    from tool_calls
    group by session_id
),

-- Combine all session metrics
sessions_computed as (
    select
        sb.session_id,
        sb.project_id,
        sb.session_start,
        sb.session_end,
        sb.start_date,
        sb.end_date,
        sb.total_entries,

        -- Duration metrics
        extract(epoch from (sb.session_end - sb.session_start)) as duration_seconds,
        extract(epoch from (sb.session_end - sb.session_start)) / 60.0 as duration_minutes,
        extract(epoch from (sb.session_end - sb.session_start)) / 3600.0 as duration_hours,

        -- Message metrics
        coalesce(ms.message_count, 0) as message_count,
        coalesce(ms.user_message_count, 0) as user_message_count,
        coalesce(ms.assistant_message_count, 0) as assistant_message_count,
        coalesce(ms.total_content_length, 0) as total_content_length,
        coalesce(ms.avg_content_length, 0) as avg_content_length,
        coalesce(ms.avg_response_time_seconds, 0) as avg_response_time_seconds,

        -- Tool metrics
        coalesce(ts.tool_call_count, 0) as tool_call_count,
        coalesce(ts.unique_tools_used, 0) as unique_tools_used,
        coalesce(ts.tool_invocations, 0) as tool_invocations,
        coalesce(ts.tool_results, 0) as tool_results,
        ts.primary_tool,

        -- Derived metrics
        case
            when ms.message_count > 0
            then coalesce(ts.tool_call_count, 0)::float / ms.message_count
            else 0
        end as tools_per_message,

        -- Session classification
        case
            when extract(epoch from (sb.session_end - sb.session_start)) < 60 then 'quick'
            when extract(epoch from (sb.session_end - sb.session_start)) < 600 then 'short'
            when extract(epoch from (sb.session_end - sb.session_start)) < 3600 then 'medium'
            else 'long'
        end as session_duration_category,

        -- Activity level
        case
            when coalesce(ms.message_count, 0) <= 5 then 'minimal'
            when coalesce(ms.message_count, 0) <= 20 then 'light'
            when coalesce(ms.message_count, 0) <= 50 then 'moderate'
            else 'heavy'
        end as activity_level

    from session_boundaries sb
    left join message_stats ms on sb.session_id = ms.session_id
    left join tool_stats ts on sb.session_id = ts.session_id
)

select * from sessions_computed
