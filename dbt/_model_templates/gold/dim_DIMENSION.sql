-- TEMPLATE — copy and rename to:
--   dbt/models/gold/<source_name>/dim_<dimension_name>.sql
-- See docs/INGESTION_GUIDE.md § Step 4c for details.

with source as (

    select distinct
        dimension_name_clean as dimension_name
    from {{ ref('slv_SOURCENAME__DATASET_enriched') }}

)

select
    {{ hashed_key(['dimension_name']) }} as dimension_sk,
    dimension_name
from source
