-- dim_company.sql
-- Dimension: hiring companies derived from Harvey Nash job postings.

with companies as (

    select distinct
        company
    from {{ ref('brz_job_market_nl__harvey_nash_vacatures') }}
    where company is not null
      and company <> ''

)

select
    {{ hashed_key(['company']) }}   as company_sk,
    company                         as company_name
from companies
