-- Superset production query templates
-- Source of truth: schema/metrics.yaml
-- Policy: production dashboards must use ACTIVE + CERTIFIED metrics only.

-- ============================================================================
-- CERTIFIED METRIC: job_market_total_vacancies
-- Formula: SUM(total_vacancies)
-- Source: job_market_nl.it_market_snapshot
-- ============================================================================
SELECT
    DATE(snapshot_date) AS metric_date,
    data_source,
    SUM(total_vacancies) AS job_market_total_vacancies
FROM job_market_nl.it_market_snapshot
GROUP BY DATE(snapshot_date), data_source
ORDER BY metric_date, data_source;
