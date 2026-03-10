-- int_fact_job_postings__fabric_aanvragen_standardized.sql
-- Canonical projection for internal staffing requests into fact_job_postings shape.

{{ config(materialized='view') }}

with aanvragen as (

    select * from {{ ref('slv_odp_staffing_demand__fabric_aanvragen_enriched') }}

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

)

select
    {{ hashed_key(["'FABRIC_LANDING'", 'a.id']) }} as posting_sk,
    a.id                                             as posting_id,
    'internal_request'                               as posting_type,
    'fabric_landing'                                 as source_system,
    dd.date_sk,
    dc.company_sk,
    dr.role_sk,
    du.unit_sk,
    dl.location_sk,
    cast(null as text)                               as region_sk,
    a.subject_edited                                 as title,
    cast(null as text)                               as description,
    cast(null as text)                               as contract_type,
    cast(null as double precision)                   as salary_min,
    cast(null as double precision)                   as salary_max,
    cast(null as text)                               as salary_raw,
    cast(null as text)                               as url,
    cast(null as timestamptz)                        as posted_date,
    a.date_received,
    a.source,
    cast(null as timestamptz)                        as ingestion_timestamp,
    a.sender_name,
    a.subject_edited,
    a.rol                                             as role_name,
    a.rol_cluster,
    a.organisatie                                     as company_name,
    a.unit                                            as unit_name,
    a.locatie_clean                                   as location_name,
    a.deadline,
    cast(null as text)                                as location,
    cast(null as text)                                as province,
    cast(null as text)                                as job_id,
    a.id                                              as aanvraag_id
from aanvragen a
left join dim_date dd      on cast(a.date_received as date) = dd.date_key
left join dim_company dc   on a.organisatie = dc.company_name
left join dim_role dr      on a.rol = dr.role_name
left join dim_unit du      on a.unit = du.unit_name
left join dim_location dl  on a.locatie_clean = dl.location_name
