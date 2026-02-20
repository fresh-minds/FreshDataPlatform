-- fact_it_market_top_skills.sql
-- Fact: skill demand with mention counts.

with skills as (

    select * from {{ ref('stg_job_market_nl__it_market_top_skills') }}

),

final as (

    select
        {{ hashed_key(['skill']) }}  as skill_sk,
        skill,
        mention_count,
        loaded_at
    from skills

)

select * from final
