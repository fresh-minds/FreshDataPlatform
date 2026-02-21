-- fct_vacatures.sql
-- Fact: Source SP1 portal vacatures with dimension surrogate key references.

with vacatures as (

    select * from {{ ref('int_source_sp1__vacatures_enriched') }}

),

dim_client as (

    select * from {{ ref('dim_client') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

dim_category as (

    select * from {{ ref('dim_category') }}

),

dim_vacancy_status as (

    select * from {{ ref('dim_vacancy_status') }}

),

final as (

    select
        {{ hashed_key(['v.vacature_id']) }}     as vacature_sk,

        -- dimension foreign keys
        dc.client_sk,
        dl.location_sk,
        dcat.category_sk,
        ds.status_sk,

        -- degenerate dimension
        v.vacature_id,

        -- descriptive attributes
        v.title,
        v.description,
        v.hours_text,
        v.source_url,

        -- dates (role-playing)
        v.publish_date,
        v.closing_date,

        -- measures
        v.hours,
        v.days_open,

        -- flags
        v.is_active,
        v.is_expired,

        -- audit
        v.updated_at_source,
        v.ingested_at

    from vacatures v
    left join dim_client dc
        on v.client_name_clean = dc.client_name
    left join dim_location dl
        on v.location_clean = dl.location_name
    left join dim_category dcat
        on v.category_clean = dcat.category_name
    left join dim_vacancy_status ds
        on v.status_clean = ds.status_name

)

select * from final
