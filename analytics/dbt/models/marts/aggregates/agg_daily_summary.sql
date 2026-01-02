-- Aggregate: Daily Summary
-- Pre-computed daily metrics for trend analysis
-- Supports time-series dashboards

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'aggregate']
    )
}}

with sessions as (
    select * from {{ ref('dim_sessions') }}
),

messages as (
    select * from {{ ref('fct_messages') }}
),

tool_calls as (
    select * from {{ ref('fct_tool_calls') }}
),

file_ops as (
    select * from {{ ref('fct_file_operations') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Session metrics per day
daily_sessions as (
    select
        date_key,
        count(*) as session_count,
        count(distinct project_id) as active_projects,
        sum(duration_minutes) as total_session_minutes,
        avg(duration_minutes) as avg_session_minutes,
        sum(message_count) as total_messages,
        avg(message_count) as avg_messages_per_session,
        sum(tool_call_count) as total_tool_calls
    from sessions
    group by date_key
),

-- Message metrics per day
daily_messages as (
    select
        date_key,
        count(*) as message_count,
        sum(case when role = 'user' then 1 else 0 end) as user_messages,
        sum(case when role = 'assistant' then 1 else 0 end) as assistant_messages,
        avg(content_length) as avg_content_length,
        sum(content_length) as total_content_length,
        sum(case when has_code_block then 1 else 0 end) as messages_with_code,
        sum(case when is_question then 1 else 0 end) as question_count
    from messages
    group by date_key
),

-- Tool metrics per day
daily_tools as (
    select
        date_key,
        count(*) as tool_call_count,
        count(distinct tool_name) as unique_tools_used,
        sum(case when is_file_operation then 1 else 0 end) as file_operations,
        sum(case when is_search_operation then 1 else 0 end) as search_operations,
        sum(case when is_shell_command then 1 else 0 end) as shell_commands,
        avg(estimated_execution_seconds) filter (
            where estimated_execution_seconds is not null
        ) as avg_execution_seconds
    from tool_calls
    group by date_key
),

-- File operation metrics per day
daily_file_ops as (
    select
        date_key,
        count(*) as file_operation_count,
        sum(case when is_read then 1 else 0 end) as file_reads,
        sum(case when is_write then 1 else 0 end) as file_writes,
        sum(case when is_edit then 1 else 0 end) as file_edits,
        count(distinct file_path) filter (where file_path is not null) as unique_files_touched
    from file_ops
    group by date_key
),

-- Combine all metrics
daily_summary as (
    select
        d.date_key,
        d.year,
        d.month,
        d.week_of_year,
        d.day_of_week,
        d.day_name,
        d.is_weekend,
        d.is_weekday,
        d.year_month,

        -- Session metrics
        coalesce(ds.session_count, 0) as session_count,
        coalesce(ds.active_projects, 0) as active_projects,
        coalesce(ds.total_session_minutes, 0) as total_session_minutes,
        round(coalesce(ds.avg_session_minutes, 0)::numeric, 2) as avg_session_minutes,

        -- Message metrics
        coalesce(dm.message_count, 0) as message_count,
        coalesce(dm.user_messages, 0) as user_messages,
        coalesce(dm.assistant_messages, 0) as assistant_messages,
        round(coalesce(dm.avg_content_length, 0)::numeric, 0) as avg_content_length,
        coalesce(dm.total_content_length, 0) as total_content_length,
        coalesce(dm.messages_with_code, 0) as messages_with_code,
        coalesce(dm.question_count, 0) as question_count,

        -- Tool metrics
        coalesce(dt.tool_call_count, 0) as tool_call_count,
        coalesce(dt.unique_tools_used, 0) as unique_tools_used,
        coalesce(dt.file_operations, 0) as file_operations,
        coalesce(dt.search_operations, 0) as search_operations,
        coalesce(dt.shell_commands, 0) as shell_commands,
        round(coalesce(dt.avg_execution_seconds, 0)::numeric, 3) as avg_tool_execution_seconds,

        -- File operation details
        coalesce(dfo.file_operation_count, 0) as file_operation_count,
        coalesce(dfo.file_reads, 0) as file_reads,
        coalesce(dfo.file_writes, 0) as file_writes,
        coalesce(dfo.file_edits, 0) as file_edits,
        coalesce(dfo.unique_files_touched, 0) as unique_files_touched,

        -- Derived metrics
        case
            when coalesce(ds.session_count, 0) > 0
            then round(coalesce(dm.message_count, 0)::numeric / ds.session_count, 2)
            else 0
        end as messages_per_session,

        case
            when coalesce(ds.session_count, 0) > 0
            then round(coalesce(dt.tool_call_count, 0)::numeric / ds.session_count, 2)
            else 0
        end as tools_per_session,

        -- Activity flags
        coalesce(ds.session_count, 0) > 0 as has_activity,

        -- Metadata
        current_timestamp as computed_at

    from dim_date d
    left join daily_sessions ds on d.date_key = ds.date_key
    left join daily_messages dm on d.date_key = dm.date_key
    left join daily_tools dt on d.date_key = dt.date_key
    left join daily_file_ops dfo on d.date_key = dfo.date_key
)

select * from daily_summary
order by date_key desc
