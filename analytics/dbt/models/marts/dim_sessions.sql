-- Session dimension for star schema
-- Contains session-level attributes and computed metrics
-- Primary analysis unit for productivity insights

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'dimension']
    )
}}

with sessions as (
    select * from {{ ref('int_sessions_computed') }}
),

messages as (
    select * from {{ ref('int_messages_enriched') }}
),

-- Get primary task category per session
session_task_category as (
    select
        session_id,
        mode() within group (order by task_category) as primary_task_category,
        count(distinct task_category) as task_category_count
    from messages
    group by session_id
),

-- Build dimension
dim_sessions as (
    select
        -- Primary key
        md5(s.session_id) as session_key,
        s.session_id,

        -- Foreign keys
        s.project_id,
        s.start_date as date_key,

        -- Session timing
        s.session_start,
        s.session_end,
        s.start_date,
        s.end_date,

        -- Duration metrics
        s.duration_seconds,
        s.duration_minutes,
        s.duration_hours,
        s.session_duration_category,

        -- Message metrics
        s.message_count,
        s.user_message_count,
        s.assistant_message_count,
        s.total_content_length,
        s.avg_content_length,
        s.avg_response_time_seconds,

        -- Tool metrics
        s.tool_call_count,
        s.unique_tools_used,
        s.tool_invocations,
        s.tool_results,
        s.primary_tool,
        s.tools_per_message,

        -- Activity classification
        s.activity_level,

        -- Task classification
        stc.primary_task_category,
        stc.task_category_count,

        -- Time-based attributes
        extract(hour from s.session_start) as start_hour,
        extract(dow from s.session_start) as start_day_of_week,

        case
            when extract(hour from s.session_start) between 6 and 11 then 'morning'
            when extract(hour from s.session_start) between 12 and 17 then 'afternoon'
            when extract(hour from s.session_start) between 18 and 21 then 'evening'
            else 'night'
        end as session_time_of_day,

        case
            when extract(dow from s.session_start) in (0, 6) then 'weekend'
            else 'weekday'
        end as session_day_type,

        -- Productivity indicators
        case
            when s.message_count > 0 and s.duration_minutes > 0
            then s.message_count / s.duration_minutes
            else 0
        end as messages_per_minute,

        case
            when s.tool_call_count > 0 and s.duration_minutes > 0
            then s.tool_call_count / s.duration_minutes
            else 0
        end as tools_per_minute

    from sessions s
    left join session_task_category stc on s.session_id = stc.session_id
)

select * from dim_sessions
