-- TEMPLATE — copy and rename to:
--   dbt_parallel/models/marts/<source_name>/dim_<dimension_name>.sql
-- See docs/INGESTION_GUIDE.md § Step 4c for details.
--
-- Convention: dimension tables derive surrogate keys using {{ hashed_key() }},
-- SELECT DISTINCT the natural key + descriptive attributes, and add a flag
-- column if applicable (e.g., is_active).

with source as (

    select distinct
        dimension_name_clean as dimension_name

    from {{ ref('int_SOURCENAME__DATASET_enriched') }}

)

select
    {{ hashed_key(['dimension_name']) }} as dimension_sk,
    dimension_name

from source
