{{ config(alias='perioden') }}
select * from {{ raw_relation('finance', 'perioden') }}
