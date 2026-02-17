{{ config(alias='dim_kostendrager') }}

select distinct
    coalesce(id::text, {{ hashed_key(["code", "description"]) }}) as kostendragerid,
    code::text as kostendragercode,
    description::text as kostendragernaam
from {{ ref('silver_finance__kostendrager') }}
