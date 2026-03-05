-- brz_job_market_nl__fl_dim_organisatie.sql
-- Bronze: type-cast and clean fabric_landing.dim_organisatie.
-- PascalCase Fabric columns are double-quoted.

with source as (

    select * from {{ source('fabric_landing', 'dim_organisatie') }}

)

select
    cast("ID" as text)                                              as id,
    nullif(nullif(trim(cast("Organisatie" as text)), ''), 'None')   as organisatie
from source
where "Organisatie" is not null
  and trim(cast("Organisatie" as text)) not in ('', 'None')
