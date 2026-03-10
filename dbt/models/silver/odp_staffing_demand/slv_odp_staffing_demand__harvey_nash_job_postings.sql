-- slv_odp_staffing_demand__harvey_nash_job_postings.sql
-- Silver model: clean Harvey Nash job postings ready for the gold layer.
--
-- Province is already extracted by the Python bronze pipeline from the card text
-- ('City, Province' format), so no city→province mapping is needed here.
-- The province field maps directly to dim_region.region_name for the FK join
-- in fact_job_postings.

with postings as (

    select * from {{ ref('brz_odp_staffing_demand__harvey_nash_vacatures') }}

),

enriched as (

    select
        p.job_id,
        p.title,
        p.company,
        p.location,
        p.province,
        p.contract_type,
        p.description,
        p.salary_min,
        p.salary_max,
        p.salary_raw,
        p.url,
        p.posted_date,
        'HARVEY_NASH'                   as source,
        p.ingestion_timestamp
    from postings p

)

select * from enriched
