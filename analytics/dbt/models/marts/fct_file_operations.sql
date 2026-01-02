-- Fact table for file operations
-- Filtered to file-related tool calls only
-- Enables code activity analysis

{{
    config(
        materialized='incremental',
        unique_key='file_operation_key',
        tags=['marts', 'gold', 'fact']
    )
}}

with tool_usage as (
    select * from {{ ref('int_tool_usage') }}
    where is_file_operation = true
    {% if is_incremental() %}
    and partition_date >= (select max(date_key) - interval '1 day' from {{ this }})
    {% endif %}
),

-- Get dimension keys
dim_sessions as (
    select session_key, session_id from {{ ref('dim_sessions') }}
),

dim_tools as (
    select tool_key, tool_name from {{ ref('dim_tools') }}
),

fct_file_operations as (
    select
        -- Primary key
        md5(tu.conversation_id) as file_operation_key,

        -- Dimension foreign keys
        ds.session_key,
        dt.tool_key,
        tu.partition_date as date_key,
        tu.project_id,

        -- Degenerate dimensions
        tu.conversation_id,
        tu.session_id,
        tu.tool_name,

        -- File operation type
        case
            when tu.tool_name = 'Read' then 'read'
            when tu.tool_name in ('Write', 'NotebookEdit') then 'write'
            when tu.tool_name = 'Edit' then 'edit'
            when tu.tool_name = 'MultiEdit' then 'multi_edit'
            else 'other'
        end as operation_type,

        -- File details
        tu.file_path,
        tu.file_extension,
        tu.file_type_category,

        -- Operation flags
        tu.tool_name = 'Read' as is_read,
        tu.tool_name = 'Write' as is_write,
        tu.tool_name = 'Edit' as is_edit,

        -- Content metrics
        tu.content_length as operation_content_length,
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

select * from fct_file_operations
