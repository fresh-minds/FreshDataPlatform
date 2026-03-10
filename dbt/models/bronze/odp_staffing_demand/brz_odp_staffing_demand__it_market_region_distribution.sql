-- brz_odp_staffing_demand__it_market_region_distribution.sql
-- Bronze model: type-cast and validate regional data.

with source as (

    select * from {{ source('odp_staffing_demand', 'it_market_region_distribution') }}

),

bronze as (

    select
        cast(trim(region) as text)                  as region,
        cast(job_ads_count as integer)               as job_ads_count,
        cast(share_pct as double precision)          as share_pct,
        cast(latitude as double precision)           as latitude,
        cast(longitude as double precision)          as longitude,
        cast(loaded_at as timestamptz)               as loaded_at
    from source
    where region is not null

)

select * from bronze
