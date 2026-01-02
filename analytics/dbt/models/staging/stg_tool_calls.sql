-- Staging model for tool calls
-- Extracts tool_use and tool_result entries
-- Parses tool name and basic parameters

{{
    config(
        materialized='view',
        tags=['staging', 'bronze']
    )
}}

with conversations as (
    select * from {{ ref('stg_conversations') }}
),

tool_entries as (
    select
        conversation_id,
        session_id,
        project_id,
        effective_timestamp,
        partition_date,

        entry_type,
        message_content,
        message_raw,
        content_length,
        source_file

    from conversations
    where is_tool_related = true
),

parsed_tools as (
    select
        conversation_id,
        session_id,
        project_id,
        effective_timestamp,
        partition_date,
        entry_type,

        -- Extract tool name from message_content or message_raw
        -- Common patterns: "Tool: ToolName" or tool invocation syntax
        case
            when message_content like '%Read%' then 'Read'
            when message_content like '%Write%' then 'Write'
            when message_content like '%Edit%' then 'Edit'
            when message_content like '%Bash%' then 'Bash'
            when message_content like '%Glob%' then 'Glob'
            when message_content like '%Grep%' then 'Grep'
            when message_content like '%Task%' then 'Task'
            when message_content like '%TodoRead%' then 'TodoRead'
            when message_content like '%TodoWrite%' then 'TodoWrite'
            when message_content like '%WebFetch%' then 'WebFetch'
            when message_content like '%WebSearch%' then 'WebSearch'
            when message_content like '%NotebookEdit%' then 'NotebookEdit'
            when message_content like '%MultiEdit%' then 'MultiEdit'
            when message_content like '%AskFollowupQuestion%' then 'AskFollowupQuestion'
            when message_content like '%AttemptCompletion%' then 'AttemptCompletion'
            else 'unknown'
        end as tool_name,

        -- Is this a tool invocation or result?
        entry_type = 'tool_use' as is_invocation,
        entry_type = 'tool_result' as is_result,

        -- Content for analysis
        message_content as tool_content,
        message_raw as raw_parameters,
        content_length,

        -- Sequence within session
        row_number() over (
            partition by session_id
            order by effective_timestamp
        ) as tool_sequence,

        source_file

    from tool_entries
)

select
    *,
    -- Try to pair tool_use with tool_result
    -- Useful for computing tool execution time
    lead(effective_timestamp) over (
        partition by session_id, tool_name
        order by effective_timestamp
    ) as next_same_tool_timestamp,

    lead(entry_type) over (
        partition by session_id
        order by effective_timestamp
    ) as next_entry_type

from parsed_tools
