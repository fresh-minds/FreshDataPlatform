-- snap_it_market_snapshot.sql
-- SCD Type 2 snapshot: track historical changes to the IT market snapshot.
-- Captures new snapshots when period_key, vacancies, or vacancy_rate change.

{% snapshot snap_it_market_snapshot %}

{{
    config(
        target_database=var('snapshot_database', target.database),
        target_schema='snapshots',
        unique_key='period_key',
        strategy='check',
        check_cols=['vacancies', 'vacancy_rate', 'job_ads_count'],
        invalidate_hard_deletes=True,
    )
}}

select
    period_key,
    period_label,
    sector_name,
    vacancies,
    vacancy_rate,
    job_ads_count,
    loaded_at
from {{ source('job_market_nl', 'it_market_snapshot') }}

{% endsnapshot %}
