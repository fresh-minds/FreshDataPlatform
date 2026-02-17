select
  skill,
  count
from {{ source('job_market_nl', 'it_market_top_skills') }}
order by count desc
