{{ config(alias='dim_administraties') }}

select distinct
    {{ hashed_key(["coalesce(administratiecode, code)", "coalesce(administratienaam, description)"]) }} as administratieid,
    coalesce(administratiecode, code)::text as administratiecode,
    coalesce(administratienaam, description)::text as administratienaam,
    country::text as land
from {{ ref('silver_finance__divisions') }}
