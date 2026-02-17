{{ config(alias='gl_classifications') }}
select * from {{ raw_relation('finance', 'gl_classifications') }}
