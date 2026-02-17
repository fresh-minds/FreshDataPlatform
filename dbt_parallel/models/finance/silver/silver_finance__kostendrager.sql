{{ config(alias='kostendrager') }}
select * from {{ raw_relation('finance', 'kostendrager') }}
