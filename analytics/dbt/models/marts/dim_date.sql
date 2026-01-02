-- Date dimension for star schema
-- Generates a date spine with various temporal attributes
-- Supports filtering and grouping in BI tools

{{
    config(
        materialized='table',
        tags=['marts', 'gold', 'dimension']
    )
}}

-- Generate date range from earliest to latest data
with date_boundaries as (
    select
        min(partition_date) as min_date,
        max(partition_date) as max_date
    from {{ ref('stg_conversations') }}
),

-- Generate date spine using DuckDB's generate_series
date_spine as (
    select
        generate_series::date as date_day
    from date_boundaries,
    generate_series(
        date_boundaries.min_date,
        date_boundaries.max_date,
        interval '1 day'
    )
),

-- Build dimension attributes
dim_date as (
    select
        -- Primary key
        date_day as date_key,

        -- ISO date components
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        extract(week from date_day) as week_of_year,
        extract(day from date_day) as day_of_month,
        extract(dow from date_day) as day_of_week,  -- 0 = Sunday
        extract(doy from date_day) as day_of_year,

        -- Readable labels
        strftime(date_day, '%Y-%m') as year_month,
        strftime(date_day, '%Y-Q') || extract(quarter from date_day)::varchar as year_quarter,
        strftime(date_day, '%B') as month_name,
        strftime(date_day, '%b') as month_name_short,
        strftime(date_day, '%A') as day_name,
        strftime(date_day, '%a') as day_name_short,

        -- Week boundaries
        date_trunc('week', date_day)::date as week_start_date,
        (date_trunc('week', date_day) + interval '6 days')::date as week_end_date,

        -- Month boundaries
        date_trunc('month', date_day)::date as month_start_date,
        (date_trunc('month', date_day) + interval '1 month' - interval '1 day')::date as month_end_date,

        -- Quarter boundaries
        date_trunc('quarter', date_day)::date as quarter_start_date,
        (date_trunc('quarter', date_day) + interval '3 months' - interval '1 day')::date as quarter_end_date,

        -- Boolean flags
        extract(dow from date_day) in (0, 6) as is_weekend,
        extract(dow from date_day) not in (0, 6) as is_weekday,
        date_day = current_date as is_today,
        date_day = current_date - interval '1 day' as is_yesterday,
        date_trunc('week', date_day) = date_trunc('week', current_date) as is_current_week,
        date_trunc('month', date_day) = date_trunc('month', current_date) as is_current_month,

        -- Relative date indicators
        current_date - date_day as days_ago,
        floor((current_date - date_day) / 7) as weeks_ago

    from date_spine
)

select * from dim_date
order by date_key
