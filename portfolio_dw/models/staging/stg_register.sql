with source as (
    select * from {{ ref('customers') }}
),

transformed as (
    select
        -- Keys
        id as customer_id,
        national_insurance_number,

        -- Personal data
        name,
        date_of_birth,
        gender,

        -- Contact details
        email,
        phone,

        -- Address
        postcode,
        city,
        county,
        country,

        -- Dates
        registration_date,

        -- Metadata
        current_timestamp as etl_inserted_at

    from source
)

select * from transformed