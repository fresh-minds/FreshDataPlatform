-- dim_vacancy_status.sql
-- Dimension: vacancy statuses from the Source SP1 portal.
-- is_active is derived from the status name semantics, not from
-- per-vacancy closing_date logic (which lives in fct_vacatures).

with statuses as (

    select distinct
        status_clean    as status_name
    from {{ ref('int_source_sp1__vacatures_enriched') }}
    where status_clean is not null

)

select
    {{ hashed_key(['status_name']) }}   as status_sk,
    status_name,
    lower(status_name) not in (
        'gesloten', 'closed', 'inactive', 'inactief',
        'expired', 'verlopen', 'cancelled', 'geannuleerd',
        'filled', 'vervuld'
    ) as is_active
from statuses
