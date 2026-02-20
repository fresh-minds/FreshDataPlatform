-- int_job_market_nl__sector_vacancy_enriched.sql
-- Intermediate model: enrich the snapshot with regional and skills context.

with snapshot as (

    select * from {{ ref('stg_job_market_nl__it_market_snapshot') }}

),

regions as (

    select
        count(*) as total_regions_with_ads,
        sum(job_ads_count) as total_regional_ads
    from {{ ref('stg_job_market_nl__it_market_region_distribution') }}

),

skills as (

    select
        count(*) as total_distinct_skills,
        sum(mention_count) as total_skill_mentions
    from {{ ref('stg_job_market_nl__it_market_top_skills') }}

),

enriched as (

    select
        s.period_key,
        s.period_label,
        s.sector_name,
        s.vacancies,
        s.vacancy_rate,
        s.job_ads_count,
        coalesce(r.total_regions_with_ads, 0)   as regions_with_ads,
        coalesce(r.total_regional_ads, 0)       as regional_ads_total,
        coalesce(sk.total_distinct_skills, 0)   as distinct_skills_count,
        coalesce(sk.total_skill_mentions, 0)    as total_skill_mentions,
        s.loaded_at
    from snapshot s
    cross join regions r
    cross join skills sk

)

select * from enriched
