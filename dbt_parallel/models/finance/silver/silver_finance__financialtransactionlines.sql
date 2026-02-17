{{ config(alias='financialtransactionlines') }}

with source_data as (
    select *
    from {{ raw_relation('finance', 'financial_transaction_lines') }}
),
base as (
    select distinct
        cast(id as text) as id,
        cast(account as text) as account,
        cast(account_name as text) as relatie_naam,
        cast(cost_center as text) as kostenplaats_code,
        cast(cost_center_description as text) as kostenplaats_description,
        cast(cost_unit as text) as kostendrager_code,
        cast(cost_unit_description as text) as kostendrager_description,
        cast(gl_account as text) as grootboek_nummer,
        cast(gl_account_code as text) as grootboekrekening_code,
        cast(gl_account_description as text) as grootboekrekening_omschrijving,
        cast(financial_period as int) as periode,
        cast(financial_year as int) as jaar,
        cast(description as text) as grootboek_omschrijving,
        cast(document as text) as grootboek_document,
        cast(document_number as text) as grootboek_document_nummer,
        cast(invoice_number as text) as grootboek_factuur_nummer,
        cast(date as text) as date_raw,
        {{ millis_to_timestamp('date') }} as datum_ts,
        cast(amount_dc as numeric(18,2)) as amount_dc,
        cast(division as text) as administratie_code,
        cast(type as text) as type
    from source_data
),
keyed as (
    select
        *,
        {{ hashed_key(["datum_ts"]) }} as date_id,
        {{ hashed_key(["jaar", "periode"]) }} as periode_id,
        {{ hashed_key(["grootboek_nummer", "grootboek_factuur_nummer", "grootboek_document_nummer", "grootboek_omschrijving"]) }} as grootboek_id,
        {{ hashed_key(["grootboekrekening_code", "administratie_code"]) }} as grootboekrekening_id,
        {{ hashed_key(["id", "administratie_code"]) }} as pk_grootboek
    from base
)
select *
from keyed
where not (
    coalesce(grootboek_omschrijving, '') = 'Resultaat 2024'
    and coalesce(nullif(grootboek_factuur_nummer, ''), '0')::bigint = 24903285
)
