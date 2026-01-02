-- Fact table for messages
-- One row per message at the finest grain
-- Links to all dimension tables

{{
    config(
        materialized='incremental',
        unique_key='message_key',
        tags=['marts', 'gold', 'fact']
    )
}}

with messages as (
    select * from {{ ref('int_messages_enriched') }}
    {% if is_incremental() %}
    where partition_date >= (select max(date_key) - interval '1 day' from {{ this }})
    {% endif %}
),

-- Get dimension keys
dim_sessions as (
    select session_key, session_id from {{ ref('dim_sessions') }}
),

fct_messages as (
    select
        -- Primary key
        md5(m.conversation_id) as message_key,

        -- Dimension foreign keys
        ds.session_key,
        m.partition_date as date_key,
        m.project_id,

        -- Degenerate dimensions (kept in fact for flexibility)
        m.conversation_id,
        m.session_id,

        -- Message attributes
        m.role,
        m.turn_type,
        m.message_sequence,

        -- Content metrics (measures)
        m.content_length,
        m.seconds_since_previous as response_time_seconds,

        -- Classification attributes
        m.task_category,
        m.time_of_day,
        m.day_type,
        m.hour_of_day,
        m.day_of_week,

        -- Content flags
        m.has_code_block,
        m.has_url,
        m.is_question,

        -- Timestamps
        m.effective_timestamp,
        m.partition_date,

        -- Source tracking
        m.source_file

    from messages m
    left join dim_sessions ds on m.session_id = ds.session_id
)

select * from fct_messages
