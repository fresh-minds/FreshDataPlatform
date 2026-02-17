{{ config(alias='financialtransactionlines_hierarchy') }}

with trans as (
    select * from {{ ref('silver_finance__financialtransactionlines') }}
),
classifications as (
    select
        cast(id as text) as id,
        cast(code as text) as code,
        cast(description as text) as description,
        cast(parent as text) as parent
    from {{ ref('silver_finance__gl_classifications') }}
),
parents as (
    select
        id as level0_id,
        code as level0_classification_code,
        description as level0_description
    from classifications
    where parent is null or nullif(parent, '') is null
),
level1 as (
    select
        p.level0_id,
        p.level0_classification_code,
        p.level0_description,
        c.id as classification_id,
        c.code as level1_classification_code,
        c.description as level1_description
    from classifications c
    join parents p on c.parent = p.level0_id
),
mappings as (
    select
        cast(gl_account as text) as gl_account,
        cast(gl_account_code as text) as gl_account_code,
        cast(classification as text) as classification_id,
        cast(gl_scheme_code as text) as gl_scheme_code
    from {{ ref('silver_finance__glaccountclassificationmappings') }}
),
joined as (
    select
        t.id,
        t.account,
        t.relatie_naam,
        t.kostenplaats_code,
        t.kostenplaats_description,
        t.kostendrager_code,
        t.kostendrager_description,
        t.grootboek_nummer,
        t.grootboekrekening_code,
        t.grootboekrekening_omschrijving,
        t.periode,
        t.jaar,
        t.grootboek_omschrijving,
        t.grootboek_document,
        t.grootboek_document_nummer,
        t.grootboek_factuur_nummer,
        t.date_raw,
        t.datum_ts,
        t.amount_dc,
        t.administratie_code,
        t.type,
        t.date_id,
        t.periode_id,
        t.grootboek_id,
        t.pk_grootboek,
        l.level0_classification_code,
        l.level0_description,
        l.level1_classification_code,
        l.level1_description,
        {{ hashed_key(["t.grootboekrekening_code", "t.administratie_code", "l.level0_classification_code", "l.level0_description", "l.level1_classification_code", "l.level1_description"]) }} as grootboekrekening_id
    from trans t
    left join mappings m
        on t.grootboek_nummer = m.gl_account
       and t.grootboekrekening_code = m.gl_account_code
       and coalesce(m.gl_scheme_code, '0') = '0'
    left join level1 l
        on m.classification_id = l.classification_id
)
select * from joined
