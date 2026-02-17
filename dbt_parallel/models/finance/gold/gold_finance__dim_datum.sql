{{ config(alias='dim_datum') }}

with dates as (
    select d::date as datum
    from generate_series('2010-01-01'::date, '2030-12-31'::date, interval '1 day') as d
)
select
    to_char(datum, 'YYYYMMDD')::int as datumid,
    {{ hashed_key(["datum"]) }} as dateid,
    datum,
    extract(year from datum)::int as jaar,
    extract(month from datum)::int as maand,
    extract(day from datum)::int as dag,
    extract(week from datum)::int as week,
    extract(quarter from datum)::int as kwartaal,
    to_char(datum, 'YYYY-MM') as jaarmaand
from dates
