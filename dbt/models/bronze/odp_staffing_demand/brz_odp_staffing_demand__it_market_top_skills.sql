-- brz_odp_staffing_demand__it_market_top_skills.sql
-- Bronze model: type-cast, trim, and deduplicate skills.

with source as (

    select * from {{ source('odp_staffing_demand', 'it_market_top_skills') }}

),

bronze as (

    select
        cast(trim(skill) as text)       as skill,
        cast(count as integer)          as mention_count,
        cast(loaded_at as timestamptz)  as loaded_at
    from source
    where skill is not null
      and trim(skill) != ''

)

select * from bronze
