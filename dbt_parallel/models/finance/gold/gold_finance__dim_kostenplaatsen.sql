{{ config(alias='dim_kostenplaatsen') }}

select distinct
    coalesce(id::text, {{ hashed_key(["code", "description"]) }}) as kostenplaatsid,
    code::text as kostenplaatscode,
    description::text as kostenplaatsnaam
from {{ ref('silver_finance__kostenplaats') }}
