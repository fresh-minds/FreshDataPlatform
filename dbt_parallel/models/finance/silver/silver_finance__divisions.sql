{{ config(alias='divisions') }}
select * from {{ raw_relation('finance', 'divisions') }}
