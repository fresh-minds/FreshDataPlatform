-- int_source_sp1__vacatures_enriched.sql
-- Intermediate: enrich vacatures with computed fields for the mart layer.

with vacatures as (

    select * from {{ ref('stg_source_sp1__vacatures') }}

),

enriched as (

    select
        vacature_id,
        title,
        status,
        client_name,
        location,
        category,
        publish_date,
        closing_date,
        hours,
        hours_text,
        description,
        source_url,
        bronze_object_path,
        checksum_sha256,
        is_active,
        updated_at_source,
        ingested_at,

        -- computed fields
        case
            when closing_date is not null and publish_date is not null
            then closing_date - publish_date
        end                                             as days_open,

        case
            when closing_date is not null
                 and closing_date < current_date
            then true
            else false
        end                                             as is_expired,

        coalesce(client_name, 'Unknown')                as client_name_clean,
        coalesce(location, 'Unknown')                   as location_clean,
        coalesce(category, 'Uncategorised')             as category_clean,
        coalesce(status, 'Unknown')                     as status_clean

    from vacatures
    where vacature_id is not null

)

select * from enriched
