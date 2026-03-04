-- dim_date.sql
-- Conformed date dimension: generates a continuous calendar from 2020-01-01
-- to 2030-12-31, covering all domains that need time-based analysis.

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

)

select
    {{ hashed_key(['date_day']) }}                          as date_sk,
    cast(date_day as date)                                  as date_key,
    extract(year from date_day)::int                        as year,
    extract(quarter from date_day)::int                     as quarter,
    extract(month from date_day)::int                       as month,
    extract(week from date_day)::int                        as week_of_year,
    extract(isodow from date_day)::int                      as day_of_week,
    (extract(isodow from date_day) in (6, 7))               as is_weekend
from date_spine
