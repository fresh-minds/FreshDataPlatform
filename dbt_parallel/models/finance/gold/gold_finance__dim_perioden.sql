{{ config(alias='dim_perioden') }}

select distinct
    {{ hashed_key(["fin_year", "fin_period"]) }} as periodeid,
    fin_year::int as jaar,
    fin_period::int as periode,
    start_date::date as startdate,
    end_date::date as enddate
from {{ ref('silver_finance__perioden') }}
