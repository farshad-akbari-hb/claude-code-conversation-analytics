-- Intermediate model for tool usage analytics
-- Parses tool call details, extracts file paths
-- Silver layer with tool-specific enrichment

{{
    config(
        materialized='view',
        tags=['intermediate', 'silver']
    )
}}

with tool_calls as (
    select * from {{ ref('stg_tool_calls') }}
),

tool_categories as (
    select * from {{ ref('tool_categories') }}
),

enriched_tools as (
    select
        tc.conversation_id,
        tc.session_id,
        tc.project_id,
        tc.effective_timestamp,
        tc.partition_date,
        tc.entry_type,
        tc.tool_name,
        tc.is_invocation,
        tc.is_result,
        tc.tool_content,
        tc.raw_parameters,
        tc.content_length,
        tc.tool_sequence,
        tc.next_same_tool_timestamp,
        tc.next_entry_type,

        -- Join with tool categories
        coalesce(cat.tool_category, 'unknown') as tool_category,
        cat.description as tool_description,

        -- Extract file paths from content (common patterns)
        case
            when tc.tool_name in ('Read', 'Write', 'Edit', 'Glob')
                and tc.tool_content like '%/%'
            then regexp_extract(tc.tool_content, '([/][a-zA-Z0-9_./-]+)', 1)
            else null
        end as file_path,

        -- Extract file extension
        case
            when tc.tool_name in ('Read', 'Write', 'Edit', 'Glob')
                and tc.tool_content like '%.%'
            then regexp_extract(tc.tool_content, '\.([a-zA-Z0-9]+)(?:[^a-zA-Z0-9]|$)', 1)
            else null
        end as file_extension,

        -- Temporal features
        extract(hour from tc.effective_timestamp) as hour_of_day,
        extract(dow from tc.effective_timestamp) as day_of_week,

        -- Tool execution time estimation (time to next event)
        case
            when tc.is_invocation
                and tc.next_entry_type = 'tool_result'
                and tc.next_same_tool_timestamp is not null
            then extract(epoch from (tc.next_same_tool_timestamp - tc.effective_timestamp))
            else null
        end as estimated_execution_seconds,

        -- Is this a file operation?
        tc.tool_name in ('Read', 'Write', 'Edit', 'NotebookEdit', 'MultiEdit') as is_file_operation,

        -- Is this a search operation?
        tc.tool_name in ('Glob', 'Grep', 'WebSearch') as is_search_operation,

        -- Is this a shell command?
        tc.tool_name = 'Bash' as is_shell_command,

        -- Source tracking
        tc.source_file

    from tool_calls tc
    left join tool_categories cat on tc.tool_name = cat.tool_name
),

-- Add file type classification
final as (
    select
        *,
        -- Classify file types
        case
            when file_extension in ('ts', 'tsx', 'js', 'jsx') then 'javascript'
            when file_extension in ('py', 'pyw') then 'python'
            when file_extension in ('java', 'kt', 'scala') then 'jvm'
            when file_extension in ('go') then 'go'
            when file_extension in ('rs') then 'rust'
            when file_extension in ('sql') then 'sql'
            when file_extension in ('md', 'mdx', 'txt', 'rst') then 'documentation'
            when file_extension in ('json', 'yaml', 'yml', 'toml', 'xml') then 'config'
            when file_extension in ('css', 'scss', 'less') then 'styles'
            when file_extension in ('html', 'htm') then 'html'
            when file_extension in ('sh', 'bash', 'zsh') then 'shell'
            when file_extension in ('dockerfile', 'docker-compose') then 'docker'
            else 'other'
        end as file_type_category

    from enriched_tools
)

select * from final
