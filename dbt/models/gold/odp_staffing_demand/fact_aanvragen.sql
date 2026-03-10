-- fact_aanvragen.sql
-- Backward-compatible projection over canonical fact_job_postings.

{{ config(materialized='view') }}

with postings as (

    select *
    from {{ ref('fact_job_postings') }}
    where posting_type = 'internal_request'

)

select
    {{ hashed_key(['posting_id']) }} as aanvraag_sk,
    date_sk,
    company_sk,
    role_sk,
    unit_sk,
    location_sk,
    posting_id                       as aanvraag_id,
    source,
    sender_name,
    subject_edited,
    role_name,
    role_cluster,
    company_name,
    unit_name,
    location_name,
    deadline,
    date_received
from postings
