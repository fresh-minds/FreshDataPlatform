{{ config(alias='openstaande_crediteuren') }}

with source_data as (
    select * from {{ raw_relation('finance', 'openstaande_crediteuren') }}
),
renamed as (
    select distinct
        cast(amount as numeric(18,2)) as totaal_bedrag,
        cast(account_name as text) as relatie_naam,
        {{ millis_to_timestamp('invoice_date') }}::date as factuurdatum_ts,
        {{ millis_to_timestamp('due_date') }}::date as vervaldatum_ts,
        cast(amount_in_transit as numeric(18,2)) as openstaand_bedrag,
        cast(description as text) as omschrijving,
        cast(invoice_number as text) as factuurnummer
    from source_data
)
select
    *,
    (current_date - vervaldatum_ts) as openstaande_dagen,
    case
        when (current_date - vervaldatum_ts) <= 30 then '0-30 dagen'
        when (current_date - vervaldatum_ts) <= 60 then '31-60 dagen'
        when (current_date - vervaldatum_ts) <= 90 then '61-90 dagen'
        when (current_date - vervaldatum_ts) > 90 then '90+ dagen'
        else 'Onbekend'
    end as agegroups
from renamed
