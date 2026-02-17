{{ config(alias='payableslist') }}
select * from {{ raw_relation('finance', 'payableslist') }}
