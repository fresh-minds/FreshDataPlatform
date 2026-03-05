-- brz_job_market_nl__fl_fact_aanvragen.sql
-- Bronze: type-cast and clean fabric_landing.fact_aanvragen.
-- Excludes PII (sender_address) and large text (text_body_clean).
-- Deduplicates on id: source data has ~173 duplicate id values where the same
-- email was matched to multiple role variants.  We keep the shortest rol text
-- (the "clean" variant without reference codes) per id.

with source as (

    select * from {{ source('fabric_landing', 'fact_aanvragen') }}

),

bronze as (

    select
        cast(id as text)                                               as id,
        nullif(nullif(trim(cast(source as text)), ''), 'None')         as source,
        nullif(nullif(trim(cast(sender_name as text)), ''), 'None')    as sender_name,
        nullif(nullif(trim(cast(subject_edited as text)), ''), 'None') as subject_edited,
        nullif(nullif(trim(cast(rol as text)), ''), 'None')            as rol,
        nullif(nullif(trim(cast(organisatie as text)), ''), 'None')    as organisatie,
        nullif(nullif(trim(cast(unit as text)), ''), 'None')           as unit,
        nullif(nullif(trim(cast(locatie_clean as text)), ''), 'None')  as locatie_clean,
        nullif(nullif(trim(cast(deadline as text)), ''), 'None')       as deadline,
        cast(date_received as timestamptz)                             as date_received,
        row_number() over (
            partition by id
            order by length(coalesce(cast(rol as text), '')) asc
        ) as _rn
    from source
    where id is not null
      and cast(id as text) <> ''
      and cast(id as text) <> 'None'

)

select
    id, source, sender_name, subject_edited, rol,
    organisatie, unit, locatie_clean, deadline, date_received
from bronze
where _rn = 1
