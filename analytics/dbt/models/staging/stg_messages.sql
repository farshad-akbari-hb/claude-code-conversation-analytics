-- Staging model for messages
-- Extracts and parses user and assistant messages
-- Filters to message-type entries only

{{
    config(
        materialized='view',
        tags=['staging', 'bronze']
    )
}}

with conversations as (
    select * from {{ ref('stg_conversations') }}
),

messages_only as (
    select
        conversation_id,
        session_id,
        project_id,
        effective_timestamp,
        partition_date,

        -- Message details
        entry_type,
        coalesce(message_role, entry_type) as role,
        message_content,
        content_length,

        -- Sequence number within session for ordering
        row_number() over (
            partition by session_id
            order by effective_timestamp
        ) as message_sequence,

        -- Is this a response to a previous message?
        lag(message_role) over (
            partition by session_id
            order by effective_timestamp
        ) as previous_role,

        -- Time since previous message in session
        extract(epoch from (
            effective_timestamp -
            lag(effective_timestamp) over (
                partition by session_id
                order by effective_timestamp
            )
        )) as seconds_since_previous,

        -- Source tracking
        source_file

    from conversations
    where is_message = true
      and message_content is not null
)

select
    *,
    -- Classify conversation turn type
    case
        when previous_role is null then 'conversation_start'
        when role = 'user' and previous_role = 'assistant' then 'follow_up'
        when role = 'assistant' and previous_role = 'user' then 'response'
        else 'continuation'
    end as turn_type

from messages_only
