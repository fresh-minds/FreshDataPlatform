-- dim_region.sql
-- Dimension: Dutch provinces with geo coordinates.

with regions as (

    select distinct
        region,
        latitude,
        longitude
    from {{ ref('brz_odp_staffing_demand__it_market_region_distribution') }}
    where region is not null

)

select
    {{ hashed_key(['region']) }}     as region_sk,
    region                           as region_name,
    latitude,
    longitude
from regions
