-- Fact table for tool calls
-- One row per tool invocation or result
-- Links to session, date, and tool dimensions

{{
    config(
        materialized='incremental',
        unique_key='tool_call_key',
        tags=['marts', 'gold', 'fact']
    )
}}

with tool_usage as (
    select * from {{ ref('int_tool_usage') }}
    {% if is_incremental() %}
    where partition_date >= (select max(date_key) - interval '1 day' from {{ this }})
    {% endif %}
),

-- Get dimension keys
dim_sessions as (
    select session_key, session_id from {{ ref('dim_sessions') }}
),

dim_tools as (
    select tool_key, tool_name from {{ ref('dim_tools') }}
),

fct_tool_calls as (
    select
        -- Primary key
        md5(tu.conversation_id) as tool_call_key,

        -- Dimension foreign keys
        ds.session_key,
        dt.tool_key,
        tu.partition_date as date_key,
        tu.project_id,

        -- Degenerate dimensions
        tu.conversation_id,
        tu.session_id,
        tu.tool_name,

        -- Tool call attributes
        tu.entry_type,
        tu.is_invocation,
        tu.is_result,
        tu.tool_sequence,

        -- Tool metadata
        tu.tool_category,
        tu.is_file_operation,
        tu.is_search_operation,
        tu.is_shell_command,

        -- File operation details
        tu.file_path,
        tu.file_extension,
        tu.file_type_category,

        -- Performance metrics
        tu.content_length as tool_content_length,
        tu.estimated_execution_seconds,

        -- Time attributes
        tu.hour_of_day,
        tu.day_of_week,

        -- Timestamps
        tu.effective_timestamp,
        tu.partition_date,

        -- Source tracking
        tu.source_file

    from tool_usage tu
    left join dim_sessions ds on tu.session_id = ds.session_id
    left join dim_tools dt on tu.tool_name = dt.tool_name
)

select * from fct_tool_calls
