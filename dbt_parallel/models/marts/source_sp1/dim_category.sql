-- dim_category.sql
-- Dimension: job categories from the Source SP1 portal.

with categories as (

    select distinct
        category_clean as category_name
    from {{ ref('int_source_sp1__vacatures_enriched') }}
    where category_clean is not null

)

select
    {{ hashed_key(['category_name']) }}     as category_sk,
    category_name
from categories
