-- slv_job_market_nl__fabric_aanvragen_enriched.sql
-- Silver: fabric aanvragen enriched with role cluster groupings from dim_rol.
-- Excludes PII columns (sender_address, text_body_clean).

with aanvragen as (

    select * from {{ ref('brz_job_market_nl__fl_fact_aanvragen') }}

),

roles as (

    select distinct rol, rol_cluster
    from {{ ref('brz_job_market_nl__fl_dim_rol') }}
    where rol is not null

),

final as (

    select
        a.id,
        a.source,
        a.sender_name,
        a.subject_edited,
        a.rol,
        r.rol_cluster,
        a.organisatie,
        a.unit,
        a.locatie_clean,
        a.deadline,
        a.date_received
    from aanvragen a
    left join roles r on a.rol = r.rol

)

select * from final
