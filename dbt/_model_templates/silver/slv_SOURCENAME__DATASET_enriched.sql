-- TEMPLATE — copy and rename to:
--   dbt/models/silver/<source_name>/slv_<source_name>__<dataset>_enriched.sql
-- See docs/INGESTION_GUIDE.md § Step 4b for details.

with bronze as (

    select * from {{ ref('brz_SOURCENAME__DATASET') }}

),

enriched as (

    select
        *,
        coalesce(nullif(trim(status), ''), 'Unknown') as status_clean
    from bronze
    where entity_id is not null

)

select * from enriched
