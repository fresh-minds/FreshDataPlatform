{{ config(alias='kostenplaats') }}
select * from {{ raw_relation('finance', 'kostenplaats') }}
