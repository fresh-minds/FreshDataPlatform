-- stg_job_market_nl__it_market_region_distribution.sql
-- Staging model: type-cast and validate regional data.

with source as (

    select * from {{ source('job_market_nl', 'it_market_region_distribution') }}

),

staged as (

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

select * from staged
