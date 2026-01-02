-- Tool dimension for star schema
-- Contains tool metadata and usage statistics
-- Supports tool usage pattern analysis

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'dimension']
    )
}}

with tool_categories as (
    select * from {{ ref('tool_categories') }}
),

tool_usage as (
    select * from {{ ref('int_tool_usage') }}
),

-- Tool usage statistics
tool_stats as (
    select
        tool_name,
        count(*) as total_calls,
        count(distinct session_id) as sessions_used,
        count(distinct project_id) as projects_used,
        count(distinct partition_date) as days_used,
        min(effective_timestamp) as first_used,
        max(effective_timestamp) as last_used,
        sum(case when is_invocation then 1 else 0 end) as invocation_count,
        sum(case when is_result then 1 else 0 end) as result_count,
        avg(estimated_execution_seconds) filter (where estimated_execution_seconds is not null) as avg_execution_seconds
    from tool_usage
    group by tool_name
),

-- File type distribution per tool
file_type_stats as (
    select
        tool_name,
        mode() within group (order by file_type_category) as primary_file_type,
        count(distinct file_type_category) as file_types_count
    from tool_usage
    where is_file_operation = true and file_type_category is not null
    group by tool_name
),

-- Build dimension
dim_tools as (
    select
        -- Primary key
        md5(tc.tool_name) as tool_key,
        tc.tool_name,

        -- Category from seed
        tc.tool_category,
        tc.description as tool_description,

        -- Computed flags
        tc.tool_category = 'file_operations' as is_file_tool,
        tc.tool_category = 'shell' as is_shell_tool,
        tc.tool_category = 'search' as is_search_tool,
        tc.tool_category = 'agent' as is_agent_tool,
        tc.tool_category = 'planning' as is_planning_tool,
        tc.tool_category = 'network' as is_network_tool,
        tc.tool_category = 'interaction' as is_interaction_tool,

        -- Usage statistics
        coalesce(ts.total_calls, 0) as total_calls,
        coalesce(ts.sessions_used, 0) as sessions_used,
        coalesce(ts.projects_used, 0) as projects_used,
        coalesce(ts.days_used, 0) as days_used,
        ts.first_used,
        ts.last_used,
        coalesce(ts.invocation_count, 0) as invocation_count,
        coalesce(ts.result_count, 0) as result_count,
        ts.avg_execution_seconds,

        -- File type stats (for file tools)
        fts.primary_file_type,
        coalesce(fts.file_types_count, 0) as file_types_count,

        -- Derived metrics
        case
            when ts.invocation_count > 0 and ts.result_count > 0
            then ts.result_count::float / ts.invocation_count
            else null
        end as success_rate,

        -- Popularity ranking
        rank() over (order by coalesce(ts.total_calls, 0) desc) as popularity_rank,

        -- Usage tier
        case
            when coalesce(ts.total_calls, 0) = 0 then 'unused'
            when ts.total_calls <= 10 then 'rare'
            when ts.total_calls <= 100 then 'occasional'
            when ts.total_calls <= 1000 then 'frequent'
            else 'heavy'
        end as usage_tier

    from tool_categories tc
    left join tool_stats ts on tc.tool_name = ts.tool_name
    left join file_type_stats fts on tc.tool_name = fts.tool_name
)

select * from dim_tools
