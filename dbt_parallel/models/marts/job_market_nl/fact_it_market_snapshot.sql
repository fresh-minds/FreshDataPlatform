-- fact_it_market_snapshot.sql
-- Fact: IT market snapshot with dimension surrogate key references.

with snapshot as (

    select * from {{ ref('int_job_market_nl__sector_vacancy_enriched') }}

),

dim_period as (

    select * from {{ ref('dim_period') }}

),

dim_sector as (

    select * from {{ ref('dim_sector') }}

),

final as (

    select
        {{ hashed_key(['s.period_key', 's.sector_name']) }} as snapshot_sk,
        dp.period_sk,
        ds.sector_sk,
        s.period_key,
        s.period_label,
        s.sector_name,
        s.vacancies,
        s.vacancy_rate,
        s.job_ads_count,
        s.regions_with_ads,
        s.regional_ads_total,
        s.distinct_skills_count,
        s.total_skill_mentions,
        s.loaded_at
    from snapshot s
    left join dim_period dp on s.period_key = dp.period_key
    left join dim_sector ds on s.sector_name = ds.sector_name

)

select * from final
