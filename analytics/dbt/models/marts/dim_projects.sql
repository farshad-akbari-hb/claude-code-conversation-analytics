-- Project dimension for star schema
-- Tracks project-level attributes and activity metrics
-- Supports project-based analytics

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'dimension']
    )
}}

with conversations as (
    select * from {{ ref('stg_conversations') }}
),

sessions as (
    select * from {{ ref('int_sessions_computed') }}
),

-- Project activity summary
project_activity as (
    select
        project_id,
        min(effective_timestamp) as first_seen,
        max(effective_timestamp) as last_active,
        count(*) as total_entries,
        count(distinct session_id) as session_count,
        count(distinct partition_date) as active_days
    from conversations
    where project_id is not null
    group by project_id
),

-- Session metrics per project
project_session_metrics as (
    select
        project_id,
        sum(message_count) as total_messages,
        sum(tool_call_count) as total_tool_calls,
        avg(duration_minutes) as avg_session_duration_minutes,
        avg(message_count) as avg_messages_per_session,
        avg(tool_call_count) as avg_tools_per_session
    from sessions
    where project_id is not null
    group by project_id
),

-- Build dimension
dim_projects as (
    select
        -- Primary key (use hash for consistency)
        md5(pa.project_id) as project_key,
        pa.project_id,

        -- Activity timestamps
        pa.first_seen,
        pa.last_active,
        pa.first_seen::date as first_seen_date,
        pa.last_active::date as last_active_date,

        -- Activity metrics
        pa.total_entries,
        pa.session_count,
        pa.active_days,

        -- Session metrics
        coalesce(psm.total_messages, 0) as total_messages,
        coalesce(psm.total_tool_calls, 0) as total_tool_calls,
        coalesce(psm.avg_session_duration_minutes, 0) as avg_session_duration_minutes,
        coalesce(psm.avg_messages_per_session, 0) as avg_messages_per_session,
        coalesce(psm.avg_tools_per_session, 0) as avg_tools_per_session,

        -- Derived metrics
        current_date - pa.last_active::date as days_since_active,
        pa.last_active::date - pa.first_seen::date as project_age_days,

        -- Activity classification
        case
            when current_date - pa.last_active::date <= 1 then 'active_today'
            when current_date - pa.last_active::date <= 7 then 'active_this_week'
            when current_date - pa.last_active::date <= 30 then 'active_this_month'
            else 'inactive'
        end as activity_status,

        -- Size classification
        case
            when pa.session_count = 1 then 'single_session'
            when pa.session_count <= 5 then 'small'
            when pa.session_count <= 20 then 'medium'
            else 'large'
        end as project_size

    from project_activity pa
    left join project_session_metrics psm on pa.project_id = psm.project_id
)

select * from dim_projects
