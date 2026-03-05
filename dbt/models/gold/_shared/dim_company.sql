-- dim_company.sql
-- Dimension: hiring companies from Harvey Nash + fabric_landing organisations.

with harvey_nash_companies as (

    select distinct
        company
    from {{ ref('brz_job_market_nl__harvey_nash_vacatures') }}
    where company is not null
      and company <> ''

),

fabric_companies as (

    select distinct
        organisatie as company
    from {{ ref('brz_job_market_nl__fl_dim_organisatie') }}
    where organisatie is not null
      and organisatie <> ''

),

all_companies as (

    select company from harvey_nash_companies
    union
    select company from fabric_companies

)

select
    {{ hashed_key(['company']) }}   as company_sk,
    company                         as company_name
from all_companies
