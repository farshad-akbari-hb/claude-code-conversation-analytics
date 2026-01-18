-- Staging model for tool calls
-- Extracts tool_use and tool_result entries
-- Uses the tool_name field populated during extraction

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
        source_file,

        -- Tool-specific fields from extraction
        tool_name,
        tool_id,
        tool_use_id

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

        -- Tool name is now directly available from extraction
        -- For tool_result records, we need to look up the tool name via tool_use_id
        coalesce(tool_name, 'unknown') as tool_name,

        -- Tool identifiers for pairing invocations with results
        tool_id,
        tool_use_id,

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
