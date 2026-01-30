{{
    config(
        materialized = 'table',
        unique_key = 'sk_customer',
        tags = ['intermediate', 'dimension']
    )
}}

with customers as (
    select * from {{ ref('stg_register') }}
)

select
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['national_insurance_number']) }} as sk_customer,

    -- Business key
    national_insurance_number,

    -- Descriptive attributes
    name,
    email,
    county,
    city,
    date_of_birth,

    -- Important dates
    registration_date,

    -- Metadata
    current_timestamp as dbt_updated_at,
    '{{ run_started_at }}' as dbt_loaded_at
from customers