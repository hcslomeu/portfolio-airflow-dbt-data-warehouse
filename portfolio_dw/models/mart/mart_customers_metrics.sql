{{
    config(
        materialized = 'table',
        unique_key = 'sk_customer',
        tags = ['mart', 'metrics']
    )
}}

with
dim_customers as (
    select * from {{ ref('int_dim_customers') }}
),

fact_orders as (
    select
        *,
        cast(order_date as date) as order_day
    from {{ ref('int_fact_orders') }}
),

dim_date as (
    select * from {{ ref('int_dim_date') }}
),

orders_by_customer as (
    select
        dc.sk_customer,
        dc.national_insurance_number,
        dc.name,
        dc.county,
        dc.city,

        -- Count metrics
        count(distinct fo.sk_order) as total_orders,

        -- Financial metrics
        sum(fo.total_order_value) as total_spent,
        avg(fo.total_order_value) as average_ticket,

        -- Important dates
        min(fo.order_date) as first_order_date,
        max(fo.order_date) as last_order_date,

        -- Temporal analysis
        min(dd.year_number) as first_purchase_year,
        max(dd.year_number) as last_purchase_year,
        count(distinct dd.year_number) as total_active_years,

        -- Seasonality
        count(distinct case when dd.month_name in ('December', 'January', 'February') then fo.sk_order end) as summer_orders,
        count(distinct case when dd.month_name in ('March', 'April', 'May') then fo.sk_order end) as autumn_orders,
        count(distinct case when dd.month_name in ('June', 'July', 'August') then fo.sk_order end) as winter_orders,
        count(distinct case when dd.month_name in ('September', 'October', 'November') then fo.sk_order end) as spring_orders,

        -- Days of the week with most purchases
        count(distinct case when dd.day_of_week_name = 'Sunday' then fo.sk_order end) as sunday_orders,
        count(distinct case when dd.day_of_week_name = 'Monday' then fo.sk_order end) as monday_orders,
        count(distinct case when dd.day_of_week_name = 'Tuesday' then fo.sk_order end) as tuesday_orders,
        count(distinct case when dd.day_of_week_name = 'Wednesday' then fo.sk_order end) as wednesday_orders,
        count(distinct case when dd.day_of_week_name = 'Thursday' then fo.sk_order end) as thursday_orders,
        count(distinct case when dd.day_of_week_name = 'Friday' then fo.sk_order end) as friday_orders,
        count(distinct case when dd.day_of_week_name = 'Saturday' then fo.sk_order end) as saturday_orders,

        -- Frequency and recency
        (current_date - max(fo.order_date)::date) as days_since_last_order,

        -- Average purchase frequency calculation (in days)
        case
            when count(fo.sk_order) > 1
            then (max(fo.order_date)::date - min(fo.order_date)::date)::float /
                 nullif(count(fo.sk_order) - 1, 0)
            else null
        end as average_frequency_days,

        -- Average value per month
        case
            when count(distinct to_char(fo.order_date, 'YYYY-MM')) > 0
            then sum(fo.total_order_value) / count(distinct to_char(fo.order_date, 'YYYY-MM'))
            else 0
        end as average_value_per_month,

        -- Purchase frequency per month
        case
            when count(distinct to_char(fo.order_date, 'YYYY-MM')) > 0
            then count(fo.sk_order)::float / count(distinct to_char(fo.order_date, 'YYYY-MM'))
            else 0
        end as average_monthly_frequency

    from dim_customers dc
    left join fact_orders fo
        on dc.sk_customer = fo.fk_customer
    left join dim_date dd
        on date_trunc('day', fo.order_date) = dd.date_day
    group by 1, 2, 3, 4, 5
)

select
    *,
    -- Full RFM analysis
    case
        when total_spent is null or total_spent = 0 then 'Inactive'
        when total_spent > 5000 and days_since_last_order <= 30 and average_monthly_frequency >= 2 then 'Champion'
        when total_spent > 3000 and days_since_last_order <= 60 then 'Loyal Customer'
        when total_spent > 1000 and days_since_last_order <= 90 then 'Potential'
        when total_spent > 0 and days_since_last_order > 180 then 'At Risk of Churn'
        when total_spent > 0 then 'Under Observation'
        else 'Inactive'
    end as rfm_segment,

    -- RFM Score (1-5, where 5 is best)
    case
        when total_spent is null or total_spent = 0 then 1
        when total_spent > 5000 then 5
        when total_spent > 3000 then 4
        when total_spent > 1000 then 3
        when total_spent > 0 then 2
        else 1
    end as value_score,

    case
        when days_since_last_order is null then 1
        when days_since_last_order <= 30 then 5
        when days_since_last_order <= 60 then 4
        when days_since_last_order <= 90 then 3
        when days_since_last_order <= 180 then 2
        else 1
    end as recency_score,

    case
        when average_monthly_frequency is null or average_monthly_frequency = 0 then 1
        when average_monthly_frequency >= 4 then 5
        when average_monthly_frequency >= 2 then 4
        when average_monthly_frequency >= 1 then 3
        when average_monthly_frequency > 0 then 2
        else 1
    end as frequency_score,

    -- Preferred season
    case
        when summer_orders > autumn_orders and summer_orders > winter_orders and summer_orders > spring_orders then 'Summer'
        when autumn_orders > summer_orders and autumn_orders > winter_orders and autumn_orders > spring_orders then 'Autumn'
        when winter_orders > summer_orders and winter_orders > autumn_orders and winter_orders > spring_orders then 'Winter'
        when spring_orders > summer_orders and spring_orders > autumn_orders and spring_orders > winter_orders then 'Spring'
        else 'No preference'
    end as preferred_season,

    -- Growth analysis
    case
        when total_active_years > 1 and total_orders > 0 then
            case
                when (select avg(total_orders::float / total_active_years)
                      from orders_by_customer
                      where total_active_years > 1) > 0
                then (total_orders::float / total_active_years) /
                     (select avg(total_orders::float / total_active_years)
                      from orders_by_customer
                      where total_active_years > 1)
                else 0
            end
        else 0
    end as growth_rate_vs_average,

    -- Metadata
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from orders_by_customer
order by
    case when total_spent is null then 1 else 0 end,  -- Inactive last
    total_spent desc  -- Highest values first