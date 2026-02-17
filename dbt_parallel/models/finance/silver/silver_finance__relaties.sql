{{ config(alias='relaties') }}
select * from {{ raw_relation('finance', 'relaties') }}
