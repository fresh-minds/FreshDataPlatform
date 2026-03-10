-- dim_location.sql
-- Dimension: distinct cleaned locations from fabric staffing requests.

with locations as (

    select distinct locatie_clean
    from {{ ref('brz_odp_staffing_demand__fl_fact_aanvragen') }}
    where locatie_clean is not null

)

select
    {{ hashed_key(['locatie_clean']) }}   as location_sk,
    locatie_clean                         as location_name
from locations
