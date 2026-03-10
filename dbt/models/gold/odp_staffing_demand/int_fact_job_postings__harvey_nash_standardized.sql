-- int_fact_job_postings__harvey_nash_standardized.sql
-- Canonical projection for external Harvey Nash postings into fact_job_postings shape.

{{ config(materialized='view') }}

with postings as (

    select * from {{ ref('slv_odp_staffing_demand__harvey_nash_job_postings') }}

),

dim_region as (

    select * from {{ ref('dim_region') }}

),

dim_company as (

    select * from {{ ref('dim_company') }}

)

select
    {{ hashed_key(["'HARVEY_NASH'", 'p.job_id']) }} as posting_sk,
    p.job_id                                          as posting_id,
    'external_posting'                                as posting_type,
    'harvey_nash'                                     as source_system,
    cast(null as text)                                as date_sk,
    dc.company_sk,
    cast(null as text)                                as role_sk,
    cast(null as text)                                as unit_sk,
    cast(null as text)                                as location_sk,
    dr.region_sk,
    p.title,
    p.description,
    p.contract_type,
    p.salary_min,
    p.salary_max,
    p.salary_raw,
    p.url,
    p.posted_date,
    cast(null as timestamptz)                         as date_received,
    p.source,
    p.ingestion_timestamp,
    cast(null as text)                                as sender_name,
    cast(null as text)                                as subject_edited,
    cast(null as text)                                as role_name,
    cast(null as text)                                as role_cluster,
    p.company                                         as company_name,
    cast(null as text)                                as unit_name,
    p.location                                        as location_name,
    cast(null as text)                                as deadline,
    p.location,
    p.province,
    p.job_id,
    cast(null as text)                                as aanvraag_id
from postings p
left join dim_region dr  on p.province = dr.region_name
left join dim_company dc on p.company  = dc.company_name
