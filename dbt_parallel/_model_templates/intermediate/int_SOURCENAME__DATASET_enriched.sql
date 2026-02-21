-- TEMPLATE — copy and rename to:
--   dbt_parallel/models/intermediate/<source_name>/int_<source_name>__<dataset>_enriched.sql
-- See docs/INGESTION_GUIDE.md § Step 4b for details.
--
-- Convention: intermediate models add computed columns, coalesce NULLs for
-- dimension keys, and apply business logic. No external joins yet.

with staged as (

    select * from {{ ref('stg_SOURCENAME__DATASET') }}

),

enriched as (

    select
        *,

        -- Example: computed fields
        -- current_date - created_date as days_since_created,
        -- created_date < current_date - interval '90 days' as is_stale,

        -- Example: coalesce dimension keys for clean joins
        -- coalesce(nullif(trim(status), ''), 'Unknown') as status_clean,
        -- coalesce(nullif(trim(category), ''), 'Uncategorised') as category_clean,

        1 as _placeholder  -- remove when adding real columns

    from staged
    where entity_id is not null

)

select * from enriched
