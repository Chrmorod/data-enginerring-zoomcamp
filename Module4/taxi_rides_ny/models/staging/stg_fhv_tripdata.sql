with fhv_tripdata as (
    select *
    from {{ source('raw_data', 'fhv_tripdata_2019') }}
    where dispatching_base_num is not null
),

renamed as (
    select
        -- identifiers
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        Affiliated_base_number as affiliated_base_id,
        dispatching_base_num as dispatching_base_id,


        -- timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropOff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(SR_Flag as integer) as sr_flag
    from fhv_tripdata
)

select count(*) from renamed