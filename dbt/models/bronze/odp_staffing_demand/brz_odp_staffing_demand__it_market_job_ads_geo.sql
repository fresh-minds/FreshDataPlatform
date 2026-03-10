-- brz_odp_staffing_demand__it_market_job_ads_geo.sql
-- Bronze model: type-cast and normalize geo data.

with source as (

    select * from {{ source('odp_staffing_demand', 'it_market_job_ads_geo') }}

),

bronze as (

    select
        cast(job_id as text)                        as job_id,
        cast(trim(region) as text)                  as region,
        cast(latitude as double precision)           as latitude,
        cast(longitude as double precision)          as longitude,
        cast(trim(location_label) as text)           as location_label,
        cast(loaded_at as timestamptz)               as loaded_at
    from source
    where job_id is not null

)

select * from bronze
