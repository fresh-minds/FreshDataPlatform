-- TEMPLATE — copy and rename to:
--   dbt_parallel/models/marts/<source_name>/fct_<dataset>.sql
-- See docs/INGESTION_GUIDE.md § Step 4c for details.
--
-- Convention: fact tables join the enriched intermediate model to all dimension
-- tables via surrogate keys. Include measures, flags, degenerate dimensions,
-- and audit columns.

with enriched as (

    select * from {{ ref('int_SOURCENAME__DATASET_enriched') }}

),

-- dim_example as (
--     select * from {{ ref('dim_example') }}
-- ),

final as (

    select
        -- Surrogate keys (join to dimensions)
        -- dim_example.example_sk,

        -- Degenerate dimension (natural key kept in fact for traceability)
        enriched.entity_id,

        -- Descriptive attributes (optional — usually live in dims)
        enriched.title,

        -- Measures
        -- enriched.amount,

        -- Flags
        -- enriched.is_active,

        -- Audit
        enriched.ingested_at

    from enriched
    -- left join dim_example
    --     on enriched.example_clean = dim_example.example_name

)

select * from final
