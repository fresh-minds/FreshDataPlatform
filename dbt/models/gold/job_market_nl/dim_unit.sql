-- dim_unit.sql
-- Dimension: business units from fabric_landing (9 units).

with units as (

    select distinct unit_naam
    from {{ ref('brz_job_market_nl__fl_dim_unit') }}
    where unit_naam is not null

)

select
    {{ hashed_key(['unit_naam']) }}   as unit_sk,
    unit_naam                         as unit_name
from units
