-- Superset production query templates
-- Source of truth: schema/metrics.yaml
-- Policy: production dashboards must use ACTIVE + CERTIFIED metrics only.

-- ============================================================================
-- CERTIFIED METRIC: finance_outstanding_amount
-- Formula: SUM(grootboekbedrag)
-- Source: finance.fact_grootboekmutaties
-- ============================================================================
SELECT
    DATE(datum_ts) AS metric_date,
    administratie_code,
    SUM(grootboekbedrag) AS finance_outstanding_amount
FROM finance.fact_grootboekmutaties
GROUP BY DATE(datum_ts), administratie_code
ORDER BY metric_date, administratie_code;
