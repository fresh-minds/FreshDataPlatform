-- brz_odp_staffing_demand__harvey_nash_vacatures.sql
-- Bronze model: type-cast and normalize Harvey Nash job postings from source.
--
-- Note: location is a city name (e.g. 'Amsterdam'); province is already extracted
-- by the Python bronze pipeline from the card text 'City, Province' format.

with source as (

    select * from {{ source('odp_staffing_demand', 'harvey_nash_vacatures') }}

),

bronze as (

    select
        cast(id as text)                                as job_id,
        trim(cast(title as text))                       as title,
        trim(cast(company as text))                     as company,
        trim(cast(location as text))                    as location,
        trim(cast(province as text))                    as province,
        trim(cast(contract_type as text))               as contract_type,
        cast(description as text)                       as description,
        cast(salary_min as double precision)            as salary_min,
        cast(salary_max as double precision)            as salary_max,
        cast(salary_raw as text)                        as salary_raw,
        cast(url as text)                               as url,
        cast(posted_date as timestamptz)                as posted_date,
        cast(ingestion_timestamp as timestamptz)        as ingestion_timestamp
    from source
    where id is not null
      and id <> ''

)

select * from bronze
