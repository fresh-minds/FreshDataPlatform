{{ config(alias='fact_grootboekmutaties') }}

with f as (
    select * from {{ ref('silver_finance__financialtransactionlines_hierarchy') }}
),
am as (
    select * from {{ ref('gold_finance__dim_administraties') }}
),
kp as (
    select * from {{ ref('gold_finance__dim_kostenplaatsen') }}
),
kd as (
    select * from {{ ref('gold_finance__dim_kostendrager') }}
),
rl as (
    select * from {{ ref('gold_finance__dim_relaties') }}
)
select
    f.pk_grootboek as grootboekmutatieid,
    f.id,
    f.grootboek_id,
    f.grootboekrekening_id,
    am.administratieid,
    f.date_id,
    f.periode_id,
    kp.kostenplaatsid,
    kd.kostendragerid,
    rl.relatieid,
    f.amount_dc as grootboekbedrag,
    f.datum_ts,
    f.jaar,
    f.periode,
    f.administratie_code,
    f.kostenplaats_code,
    f.kostendrager_code,
    f.relatie_naam,
    f.type
from f
left join am on f.administratie_code = am.administratiecode
left join kp on f.kostenplaats_code = kp.kostenplaatscode
left join kd on f.kostendrager_code = kd.kostendragercode
left join rl on f.relatie_naam = rl.relatienaam
