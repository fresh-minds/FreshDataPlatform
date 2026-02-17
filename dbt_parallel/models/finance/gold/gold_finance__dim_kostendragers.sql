{{ config(alias='dim_kostendragers') }}

select * from {{ ref('gold_finance__dim_kostendrager') }}
