-- dim_sector.sql
-- Dimension: industry sectors extracted from snapshot data.

with sectors as (

    select distinct
        sector_name
    from {{ ref('stg_job_market_nl__it_market_snapshot') }}
    where sector_name is not null

)

select
    {{ hashed_key(['sector_name']) }}    as sector_sk,
    sector_name
from sectors
