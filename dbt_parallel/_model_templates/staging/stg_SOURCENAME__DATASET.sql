-- TEMPLATE — copy and rename to:
--   dbt_parallel/models/staging/<source_name>/stg_<source_name>__<dataset>.sql
-- See docs/INGESTION_GUIDE.md § Step 4a for details.
--
-- Convention: staging models are 1:1 views over the Postgres source table,
-- performing light type-casting, renaming, and filtering only.
-- No joins, no aggregations, no business logic.

with source as (

    select *
    from {{ source('SOURCENAME', 'DATASET') }}

),

renamed as (

    select
        -- Primary key
        entity_id,

        -- Descriptive attributes
        title,
        status,

        -- Dates (cast from text if needed)
        created_date,

        -- Measures
        -- amount::numeric as amount,

        -- Metadata
        description,
        source_url,
        bronze_object_path,
        checksum_sha256,
        ingested_at

    from source
    -- Optional: filter out test or invalid rows
    -- where entity_id is not null

)

select * from renamed
