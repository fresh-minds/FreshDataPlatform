-- TEMPLATE — copy and rename to:
--   dbt/models/gold/<source_name>/fct_<dataset>.sql
-- See docs/INGESTION_GUIDE.md § Step 4c for details.

with silver as (

    select * from {{ ref('slv_SOURCENAME__DATASET_enriched') }}

)

select
    silver.entity_id,
    silver.title,
    silver.ingested_at
from silver
