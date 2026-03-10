-- dim_period.sql
-- Dimension: time periods extracted from snapshot data.

with periods as (

    select distinct
        period_key,
        period_label
    from {{ ref('brz_odp_staffing_demand__it_market_snapshot') }}

)

select
    {{ hashed_key(['period_key']) }}     as period_sk,
    period_key,
    period_label
from periods
