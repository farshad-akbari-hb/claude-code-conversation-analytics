-- Aggregate: Code Changes Analysis
-- Pre-computed file operation metrics
-- Supports code activity and modification tracking

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'aggregate']
    )
}}

with file_ops as (
    select * from {{ ref('fct_file_operations') }}
),

-- Overall file operation statistics
overall_stats as (
    select
        'overall' as aggregation_level,
        null as aggregation_key,
        count(*) as total_operations,
        sum(case when is_read then 1 else 0 end) as read_operations,
        sum(case when is_write then 1 else 0 end) as write_operations,
        sum(case when is_edit then 1 else 0 end) as edit_operations,
        count(distinct file_path) filter (where file_path is not null) as unique_files,
        count(distinct file_extension) filter (where file_extension is not null) as unique_extensions,
        count(distinct session_id) as sessions_with_file_ops,
        count(distinct project_id) as projects_with_file_ops,
        avg(operation_content_length) as avg_content_length,
        sum(operation_content_length) as total_content_length
    from file_ops
),

-- By file type category
by_file_type as (
    select
        'by_file_type' as aggregation_level,
        file_type_category as aggregation_key,
        count(*) as total_operations,
        sum(case when is_read then 1 else 0 end) as read_operations,
        sum(case when is_write then 1 else 0 end) as write_operations,
        sum(case when is_edit then 1 else 0 end) as edit_operations,
        count(distinct file_path) filter (where file_path is not null) as unique_files,
        count(distinct file_extension) filter (where file_extension is not null) as unique_extensions,
        count(distinct session_id) as sessions_with_file_ops,
        count(distinct project_id) as projects_with_file_ops,
        avg(operation_content_length) as avg_content_length,
        sum(operation_content_length) as total_content_length
    from file_ops
    where file_type_category is not null
    group by file_type_category
),

-- By operation type
by_operation_type as (
    select
        'by_operation_type' as aggregation_level,
        operation_type as aggregation_key,
        count(*) as total_operations,
        sum(case when is_read then 1 else 0 end) as read_operations,
        sum(case when is_write then 1 else 0 end) as write_operations,
        sum(case when is_edit then 1 else 0 end) as edit_operations,
        count(distinct file_path) filter (where file_path is not null) as unique_files,
        count(distinct file_extension) filter (where file_extension is not null) as unique_extensions,
        count(distinct session_id) as sessions_with_file_ops,
        count(distinct project_id) as projects_with_file_ops,
        avg(operation_content_length) as avg_content_length,
        sum(operation_content_length) as total_content_length
    from file_ops
    group by operation_type
),

-- By file extension (top extensions)
by_extension as (
    select
        'by_extension' as aggregation_level,
        file_extension as aggregation_key,
        count(*) as total_operations,
        sum(case when is_read then 1 else 0 end) as read_operations,
        sum(case when is_write then 1 else 0 end) as write_operations,
        sum(case when is_edit then 1 else 0 end) as edit_operations,
        count(distinct file_path) filter (where file_path is not null) as unique_files,
        1 as unique_extensions,
        count(distinct session_id) as sessions_with_file_ops,
        count(distinct project_id) as projects_with_file_ops,
        avg(operation_content_length) as avg_content_length,
        sum(operation_content_length) as total_content_length
    from file_ops
    where file_extension is not null
    group by file_extension
    order by count(*) desc
    limit 20
),

-- Combine all aggregations
combined as (
    select * from overall_stats
    union all
    select * from by_file_type
    union all
    select * from by_operation_type
    union all
    select * from by_extension
),

final as (
    select
        aggregation_level,
        aggregation_key,
        total_operations,
        read_operations,
        write_operations,
        edit_operations,

        -- Calculated percentages
        round(read_operations::numeric / nullif(total_operations, 0) * 100, 2) as read_pct,
        round(write_operations::numeric / nullif(total_operations, 0) * 100, 2) as write_pct,
        round(edit_operations::numeric / nullif(total_operations, 0) * 100, 2) as edit_pct,

        unique_files,
        unique_extensions,
        sessions_with_file_ops,
        projects_with_file_ops,
        round(avg_content_length::numeric, 0) as avg_content_length,
        total_content_length,

        -- Metadata
        current_timestamp as computed_at

    from combined
)

select * from final
