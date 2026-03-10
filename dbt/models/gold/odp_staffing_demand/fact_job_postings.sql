-- fact_job_postings.sql
-- Canonical gold fact for both external job postings and internal staffing requests.

with external_postings as (

    select * from {{ ref('int_fact_job_postings__harvey_nash_standardized') }}

),

internal_requests as (

    select * from {{ ref('int_fact_job_postings__fabric_aanvragen_standardized') }}

)

select * from external_postings
union all
select * from internal_requests
