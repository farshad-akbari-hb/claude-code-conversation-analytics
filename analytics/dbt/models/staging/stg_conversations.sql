-- Staging model for conversations
-- Basic cleaning, type casting, and null handling
-- This is the foundation for all downstream models

{{
    config(
        materialized='view',
        tags=['staging', 'bronze']
    )
}}

with source as (
    select * from {{ source('raw', 'conversations') }}
),

cleaned as (
    select
        -- Primary key
        _id as conversation_id,

        -- Core attributes
        coalesce(type, 'unknown') as entry_type,
        session_id,
        project_id,

        -- Timestamps with null handling
        timestamp as original_timestamp,
        coalesce(timestamp, ingested_at, extracted_at) as effective_timestamp,
        ingested_at,
        extracted_at,

        -- Message content
        message_role,
        message_content,
        message_raw,

        -- Source tracking
        source_file,
        date as partition_date,

        -- Computed fields
        case
            when message_content is not null
            then length(message_content)
            else 0
        end as content_length,

        -- Boolean flags for common queries
        type in ('user', 'assistant') as is_message,
        type in ('tool_use', 'tool_result') as is_tool_related,
        message_role = 'user' as is_user_message,
        message_role = 'assistant' as is_assistant_message

    from source
    where _id is not null  -- Ensure we have valid records
)

select * from cleaned
