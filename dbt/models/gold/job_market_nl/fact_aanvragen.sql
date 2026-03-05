-- fact_aanvragen.sql
-- Fact: fabric_landing staffing requests with 5 dimension FK references.
-- date_sk joins to shared dim_date; company_sk joins to shared dim_company.

with aanvragen as (

    select * from {{ ref('slv_job_market_nl__fabric_aanvragen_enriched') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_company as (

    select * from {{ ref('dim_company') }}

),

dim_role as (

    select * from {{ ref('dim_role') }}

),

dim_unit as (

    select * from {{ ref('dim_unit') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

final as (

    select
        {{ hashed_key(['a.id']) }}      as aanvraag_sk,
        dd.date_sk,
        dc.company_sk,
        dr.role_sk,
        du.unit_sk,
        dl.location_sk,
        a.id                            as aanvraag_id,
        a.source,
        a.sender_name,
        a.subject_edited,
        a.rol                           as role_name,
        a.rol_cluster                   as role_cluster,
        a.organisatie                    as company_name,
        a.unit                          as unit_name,
        a.locatie_clean                 as location_name,
        a.deadline,
        a.date_received
    from aanvragen a
    left join dim_date     dd on cast(a.date_received as date) = dd.date_key
    left join dim_company  dc on a.organisatie     = dc.company_name
    left join dim_role     dr on a.rol             = dr.role_name
    left join dim_unit     du on a.unit            = du.unit_name
    left join dim_location dl on a.locatie_clean   = dl.location_name

)

select * from final
