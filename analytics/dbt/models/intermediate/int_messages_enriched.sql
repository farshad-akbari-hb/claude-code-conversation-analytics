-- Intermediate model for enriched messages
-- Adds task category classification, temporal features
-- Silver layer with business logic

{{
    config(
        materialized='view',
        tags=['intermediate', 'silver']
    )
}}

with messages as (
    select * from {{ ref('stg_messages') }}
),

enriched as (
    select
        -- Keys
        conversation_id,
        session_id,
        project_id,

        -- Message details
        role,
        message_content,
        content_length,
        message_sequence,
        turn_type,
        previous_role,
        seconds_since_previous,

        -- Temporal features
        effective_timestamp,
        partition_date,

        -- Time-of-day analysis
        extract(hour from effective_timestamp) as hour_of_day,
        extract(dow from effective_timestamp) as day_of_week,  -- 0 = Sunday
        extract(week from effective_timestamp) as week_of_year,
        extract(month from effective_timestamp) as month_of_year,

        -- Time classification
        case
            when extract(hour from effective_timestamp) between 6 and 11 then 'morning'
            when extract(hour from effective_timestamp) between 12 and 17 then 'afternoon'
            when extract(hour from effective_timestamp) between 18 and 21 then 'evening'
            else 'night'
        end as time_of_day,

        case
            when extract(dow from effective_timestamp) in (0, 6) then 'weekend'
            else 'weekday'
        end as day_type,

        -- Task category classification based on content patterns
        case
            -- Bug fixing patterns
            when lower(message_content) like '%fix%'
                or lower(message_content) like '%bug%'
                or lower(message_content) like '%error%'
                or lower(message_content) like '%issue%'
                or lower(message_content) like '%broken%'
                or lower(message_content) like '%not working%'
            then 'bug_fix'

            -- Feature development patterns
            when lower(message_content) like '%add%feature%'
                or lower(message_content) like '%implement%'
                or lower(message_content) like '%create%'
                or lower(message_content) like '%build%'
                or lower(message_content) like '%new%function%'
            then 'feature'

            -- Refactoring patterns
            when lower(message_content) like '%refactor%'
                or lower(message_content) like '%clean up%'
                or lower(message_content) like '%improve%'
                or lower(message_content) like '%optimize%'
                or lower(message_content) like '%restructure%'
            then 'refactor'

            -- Testing patterns
            when lower(message_content) like '%test%'
                or lower(message_content) like '%spec%'
                or lower(message_content) like '%coverage%'
                or lower(message_content) like '%assert%'
            then 'testing'

            -- Documentation patterns
            when lower(message_content) like '%document%'
                or lower(message_content) like '%readme%'
                or lower(message_content) like '%comment%'
                or lower(message_content) like '%explain%'
            then 'documentation'

            -- Code review patterns
            when lower(message_content) like '%review%'
                or lower(message_content) like '%look at%'
                or lower(message_content) like '%check%'
                or lower(message_content) like '%what does%'
            then 'review'

            else 'other'
        end as task_category,

        -- Content indicators
        message_content like '%```%' as has_code_block,
        message_content like '%http%' as has_url,
        message_content like '%?%' as is_question,

        -- Source tracking
        source_file

    from messages
)

select * from enriched
