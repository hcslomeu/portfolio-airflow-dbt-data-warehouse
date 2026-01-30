with source as (
    select * from {{ ref('orders') }}
),

transformed as (
    select
        -- Keys
        order_id,
        national_insurance_number,

        -- Monetary values
        order_value,
        shipping_cost,
        discount_amount,
        (order_value + shipping_cost - coalesce(discount_amount, 0)) as total_order_value,

        -- Coupon
        coupon,
        case when coupon is not null then true else false end as has_coupon,

        -- Delivery address
        delivery_street,
        delivery_number,
        delivery_district,
        delivery_city,
        delivery_county,
        delivery_country,

        -- Status and dates
        order_status,
        order_date,

        -- Metadata
        current_timestamp as etl_inserted_at

    from source
)

select * from transformed