-- brz_job_market_nl__fl_dim_rol.sql
-- Bronze: type-cast and clean fabric_landing.dim_rol.
-- PascalCase Fabric columns are double-quoted.

with source as (

    select * from {{ source('fabric_landing', 'dim_rol') }}

)

select
    cast("ID" as text)                                           as id,
    nullif(nullif(trim(cast("Rol" as text)), ''), 'None')        as rol,
    nullif(nullif(trim(cast("RolCluster" as text)), ''), 'None') as rol_cluster
from source
where "Rol" is not null
  and trim(cast("Rol" as text)) not in ('', 'None')
