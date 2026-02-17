{{ config(alias='receivableslist') }}
select * from {{ raw_relation('finance', 'receivableslist') }}
