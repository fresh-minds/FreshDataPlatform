{{ config(alias='gl_accounts') }}
select * from {{ raw_relation('finance', 'gl_accounts') }}
