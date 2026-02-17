{{ config(alias='dim_relaties') }}

select distinct
    coalesce(id::text, {{ hashed_key(["name", "code"]) }}) as relatieid,
    name::text as relatienaam,
    code::text as relatiecode
from {{ ref('silver_finance__relaties') }}
