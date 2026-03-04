-- fact_job_postings.sql
-- Fact: individual Harvey Nash job postings with dimension surrogate key references.

with postings as (

    select * from {{ ref('slv_job_market_nl__harvey_nash_job_postings') }}

),

dim_region as (

    select * from {{ ref('dim_region') }}

),

dim_company as (

    select * from {{ ref('dim_company') }}

),

final as (

    select
        {{ hashed_key(['p.job_id']) }}          as posting_sk,
        dr.region_sk,
        dc.company_sk,
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
        p.source,
        p.ingestion_timestamp
    from postings p
    -- Province is pre-resolved by the Python pipeline; direct string match to dim_region.
    left join dim_region dr  on p.province = dr.region_name
    left join dim_company dc on p.company  = dc.company_name

)

select * from final
