{{ config(alias='glaccountclassificationmappings') }}
select * from {{ raw_relation('finance', 'glaccountclassificationmappings') }}
