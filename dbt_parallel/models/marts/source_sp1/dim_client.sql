-- dim_client.sql
-- Dimension: client companies posting vacancies on the Source SP1 portal.

with clients as (

    select distinct
        client_name_clean as client_name
    from {{ ref('int_source_sp1__vacatures_enriched') }}
    where client_name_clean is not null

)

select
    {{ hashed_key(['client_name']) }}   as client_sk,
    client_name
from clients
