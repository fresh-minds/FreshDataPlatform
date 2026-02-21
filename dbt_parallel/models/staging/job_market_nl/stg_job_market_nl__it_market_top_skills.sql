-- stg_job_market_nl__it_market_top_skills.sql
-- Staging model: type-cast, trim, and deduplicate skills.

with source as (

    select * from {{ source('job_market_nl', 'it_market_top_skills') }}

),

staged as (

    select
        cast(trim(skill) as text)       as skill,
        cast(count as integer)          as mention_count,
        cast(loaded_at as timestamptz)  as loaded_at
    from source
    where skill is not null
      and trim(skill) != ''

)

select * from staged
