-- stg_source_sp1__vacatures.sql
-- Staging model: type-cast, rename, and filter source vacatures.
--
-- Source table: source_sp1.vacatures  (written by Python pipeline)
-- Output:       staging schema view (name: stg_source_sp1__vacatures)
--
-- Only rows with a non-null vacature_id and ingested within the last
-- INCREMENTAL_LOOKBACK_DAYS days (or ever, for historical backfill)
-- are included. Downstream mart models should filter further by is_active.

with source as (

    select * from {{ source('source_sp1', 'vacatures') }}

),

staged as (

    select
        -- Primary key
        cast(vacature_id        as text)                    as vacature_id,

        -- Descriptive fields
        cast(title              as text)                    as title,
        cast(status             as text)                    as status,
        cast(client_name        as text)                    as client_name,
        cast(location           as text)                    as location,

        -- Dates
        cast(publish_date       as date)                    as publish_date,
        cast(closing_date       as date)                    as closing_date,

        -- Hours
        cast(hours              as numeric)                 as hours,
        cast(hours_text         as text)                    as hours_text,

        -- Content
        cast(description        as text)                    as description,
        cast(category           as text)                    as category,

        -- Source timestamps
        cast(updated_at_source  as timestamp)               as updated_at_source,
        cast(ingested_at        as timestamp)               as ingested_at,

        -- Provenance
        cast(source_url         as text)                    as source_url,
        cast(bronze_object_path as text)                    as bronze_object_path,
        cast(checksum_sha256    as text)                    as checksum_sha256,

        -- Derived
        cast(is_active          as boolean)                 as is_active

    from source
    where vacature_id is not null

)

select * from staged
