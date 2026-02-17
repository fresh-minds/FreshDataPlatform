{{ config(alias='begroting') }}

select
    {{ hashed_key(["coalesce(cast(kostenplaatscode as text), '')", "coalesce(cast(kostendragercode as text), '')", "coalesce(cast(amount as text), '')", "coalesce(cast(jaar as text), '')"]) }} as id,
    *
from {{ raw_relation('finance', 'begroting') }}
