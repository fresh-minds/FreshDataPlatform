select
  period_key,
  period_label,
  sector_name,
  vacancies,
  vacancy_rate,
  job_ads_count
from {{ source('job_market_nl', 'it_market_snapshot') }}
