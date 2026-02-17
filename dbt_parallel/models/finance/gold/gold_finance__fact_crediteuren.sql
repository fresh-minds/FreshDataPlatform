{{ config(alias='fact_crediteuren') }}

with base as (
    select * from {{ ref('silver_finance__openstaande_crediteuren') }}
),
rel as (
    select * from {{ ref('gold_finance__dim_relaties') }}
)
select
    {{ hashed_key(["factuurnummer", "relatie_naam", "totaal_bedrag"]) }} as crediteurid,
    factuurnummer,
    relatie_naam,
    rel.relatieid,
    totaal_bedrag,
    openstaand_bedrag,
    factuurdatum_ts,
    vervaldatum_ts,
    openstaande_dagen,
    agegroups,
    omschrijving
from base
left join rel on base.relatie_naam = rel.relatienaam
