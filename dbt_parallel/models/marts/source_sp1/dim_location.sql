-- dim_location.sql
-- Dimension: vacancy locations from the Source SP1 portal.

with locations as (

    select distinct
        location_clean as location_name
    from {{ ref('int_source_sp1__vacatures_enriched') }}
    where location_clean is not null

)

select
    {{ hashed_key(['location_name']) }}     as location_sk,
    location_name
from locations
