{{
    config(
        materialized = 'table',
        unique_key = 'sk_order',
        tags = ['intermediate', 'fact']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customers as (
    select sk_customer, national_insurance_number from {{ ref('int_dim_customers') }}
),

dim_date as (
    select date_day from {{ ref('int_dim_date') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as sk_order,

    -- Foreign keys
    dc.sk_customer as fk_customer,

    -- Business key
    o.order_id,

    -- Date/time dimensions
    o.order_date,
    date_trunc('day', o.order_date) as order_day,

    -- Metrics
    o.total_order_value,

    -- Metadata
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from orders o
left join dim_customers dc on o.national_insurance_number = dc.national_insurance_number
left join dim_date dd on date_trunc('day', o.order_date) = dd.date_day
