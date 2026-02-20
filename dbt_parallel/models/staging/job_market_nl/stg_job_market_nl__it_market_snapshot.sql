-- stg_job_market_nl__it_market_snapshot.sql
-- Staging model: type-cast and rename source columns for downstream use.

with source as (

    select * from {{ source('job_market_nl', 'it_market_snapshot') }}

),

staged as (

    select
        cast(period_key as text)                    as period_key,
        cast(period_label as text)                  as period_label,
        cast(sector_name as text)                   as sector_name,
        cast(vacancies as double precision)          as vacancies,
        cast(vacancy_rate as double precision)       as vacancy_rate,
        cast(job_ads_count as integer)               as job_ads_count,
        cast(loaded_at as timestamptz)               as loaded_at
    from source
    where period_key is not null

)

select * from staged
