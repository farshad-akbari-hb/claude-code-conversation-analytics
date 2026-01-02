-- Aggregate: Tool Efficiency Analysis
-- Pre-computed tool usage and performance metrics
-- Supports AI interaction pattern analysis

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'aggregate']
    )
}}

with tool_calls as (
    select * from {{ ref('fct_tool_calls') }}
),

dim_tools as (
    select * from {{ ref('dim_tools') }}
),

-- Per-tool statistics
tool_stats as (
    select
        tc.tool_name,
        tc.tool_category,
        count(*) as total_calls,
        count(distinct tc.session_id) as sessions_used,
        count(distinct tc.project_id) as projects_used,
        sum(case when tc.is_invocation then 1 else 0 end) as invocations,
        sum(case when tc.is_result then 1 else 0 end) as results,
        avg(tc.estimated_execution_seconds) filter (
            where tc.estimated_execution_seconds is not null
        ) as avg_execution_seconds,
        percentile_cont(0.5) within group (
            order by tc.estimated_execution_seconds
        ) filter (
            where tc.estimated_execution_seconds is not null
        ) as median_execution_seconds,
        max(tc.estimated_execution_seconds) as max_execution_seconds
    from tool_calls tc
    group by tc.tool_name, tc.tool_category
),

-- Category-level statistics
category_stats as (
    select
        tool_category,
        count(*) as total_calls,
        count(distinct tool_name) as unique_tools,
        count(distinct session_id) as sessions_used,
        avg(estimated_execution_seconds) filter (
            where estimated_execution_seconds is not null
        ) as avg_execution_seconds
    from tool_calls
    group by tool_category
),

-- Hourly distribution
hourly_distribution as (
    select
        tool_name,
        hour_of_day,
        count(*) as call_count
    from tool_calls
    group by tool_name, hour_of_day
),

-- Peak hour per tool
peak_hours as (
    select distinct on (tool_name)
        tool_name,
        hour_of_day as peak_hour,
        call_count as peak_hour_calls
    from hourly_distribution
    order by tool_name, call_count desc
),

-- Final tool efficiency metrics
final as (
    select
        ts.tool_name,
        ts.tool_category,
        dt.tool_description,

        -- Usage metrics
        ts.total_calls,
        ts.sessions_used,
        ts.projects_used,
        ts.invocations,
        ts.results,

        -- Success rate (results / invocations)
        case
            when ts.invocations > 0
            then round(ts.results::numeric / ts.invocations * 100, 2)
            else null
        end as success_rate_pct,

        -- Performance metrics
        round(ts.avg_execution_seconds::numeric, 3) as avg_execution_seconds,
        round(ts.median_execution_seconds::numeric, 3) as median_execution_seconds,
        round(ts.max_execution_seconds::numeric, 3) as max_execution_seconds,

        -- Distribution
        ph.peak_hour,
        ph.peak_hour_calls,

        -- Ranking
        dt.popularity_rank,
        dt.usage_tier,

        -- Category totals
        cs.total_calls as category_total_calls,
        round(ts.total_calls::numeric / nullif(cs.total_calls, 0) * 100, 2) as pct_of_category,

        -- Metadata
        current_timestamp as computed_at

    from tool_stats ts
    left join dim_tools dt on ts.tool_name = dt.tool_name
    left join category_stats cs on ts.tool_category = cs.tool_category
    left join peak_hours ph on ts.tool_name = ph.tool_name
)

select * from final
order by total_calls desc
