-- TEMPLATE — copy and rename to:
--   dbt/models/bronze/<source_name>/brz_<source_name>__<dataset>.sql
-- See docs/INGESTION_GUIDE.md § Step 4a for details.
--
-- Convention: bronze dbt models are 1:1 views over source tables,
-- performing light type-casting and column normalization only.

with source as (

    select *
    from {{ source('SOURCENAME', 'DATASET') }}

),

normalized as (

    select
        entity_id,
        title,
        status,
        created_date,
        description,
        source_url,
        bronze_object_path,
        checksum_sha256,
        ingested_at
    from source

)

select * from normalized
