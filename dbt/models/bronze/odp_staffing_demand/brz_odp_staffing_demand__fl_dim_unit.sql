-- brz_odp_staffing_demand__fl_dim_unit.sql
-- Bronze: type-cast and clean fabric_landing.dim_unit.
-- PascalCase Fabric columns are double-quoted.

with source as (

    select * from {{ source('fabric_landing', 'dim_unit') }}

)

select
    cast("ID" as text)                                             as id,
    nullif(nullif(trim(cast("UnitNaam" as text)), ''), 'None')     as unit_naam
from source
where "UnitNaam" is not null
  and trim(cast("UnitNaam" as text)) not in ('', 'None')
