{{
    config(
        materialized = 'table',
        unique_key = 'date_day',
        tags = ['mart', 'metrics']
    )
}}

with fact_orders as (
    select
        *,
        cast(order_date as date) as order_day
    from {{ ref('int_fact_orders') }}
),

dim_date as (
    select * from {{ ref('int_dim_date') }}
),

sales_by_day as (
    select
        d.date_day,

        -- Temporal dimensions
        d.day_of_week_name as day_of_week,
        d.month_name as month,
        d.quarter_of_year as quarter,
        d.year_number as year,

        -- Basic metrics
        count(distinct o.sk_order) as total_orders,
        count(distinct o.fk_customer) as unique_customers,

        -- Financial metrics
        sum(o.total_order_value) as gross_revenue,
        avg(o.total_order_value) as average_ticket,

        -- Value per customer metrics
        sum(o.total_order_value) / nullif(count(distinct o.fk_customer), 0) as revenue_per_customer,

        -- Growth metrics
        lag(sum(o.total_order_value)) over (order by d.date_day) as previous_day_revenue,
        lag(count(distinct o.sk_order)) over (order by d.date_day) as previous_day_orders

    from dim_date d
    left join fact_orders o
        on d.date_day = date_trunc('day', o.order_date)
    where d.date_day between (select min(date_trunc('day', order_date)) from fact_orders)
                         and (select max(date_trunc('day', order_date)) from fact_orders)
    group by 1, 2, 3, 4, 5
),

sales_with_metrics as (
    select
        *,
        -- 7-day rolling average
        avg(gross_revenue) over (
            order by date_day
            rows between 6 preceding and current row
        ) as rolling_7d_revenue,

        -- Previous day variance
        gross_revenue - previous_day_revenue as previous_day_variance,

        -- Growth rate
        case
            when previous_day_revenue > 0
            then (gross_revenue - previous_day_revenue) / previous_day_revenue
            else null
        end as revenue_growth_previous_day,

        -- Month/year data
        to_char(date_day, 'YYYY-MM') as year_month
    from sales_by_day
)

select
    sd.*,
    -- Percentage variance (already calculated in previous CTE)
    (coalesce(sd.revenue_growth_previous_day * 100, 0))::numeric(10,2) as previous_day_percentage_variance,

    -- Comparison with same day of previous week
    lag(sd.gross_revenue, 7) over (order by sd.date_day) as same_week_previous_revenue,

    -- Comparison with same month of previous year
    lag(sd.gross_revenue, 12) over (partition by dd.month_of_year order by sd.date_day) as same_month_previous_year_revenue,

    -- Seasonality (average of last 3 years for same month)
    avg(sd.gross_revenue) over (
        partition by dd.month_of_year
        order by sd.date_day
        rows between 2 preceding and current row
    ) as seasonal_rolling_avg_3years,

    -- Metadata
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from sales_with_metrics sd
left join dim_date dd on sd.date_day = dd.date_day
order by sd.date_day desc