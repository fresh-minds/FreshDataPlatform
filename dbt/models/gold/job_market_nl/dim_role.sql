-- dim_role.sql
-- Dimension: job roles with cluster groupings from fabric_landing.

with roles as (

    select distinct rol, rol_cluster
    from {{ ref('brz_job_market_nl__fl_dim_rol') }}
    where rol is not null

)

select
    {{ hashed_key(['rol']) }}   as role_sk,
    rol                         as role_name,
    rol_cluster                 as role_cluster
from roles
