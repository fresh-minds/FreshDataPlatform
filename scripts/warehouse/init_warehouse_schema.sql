-- Initialize schema for dashboard-ready job market views

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS job_market_nl;

-- Create denormalized view for BI
CREATE OR REPLACE VIEW job_market_nl.vw_it_market_snapshot_full AS
SELECT
    s.snapshot_date,
    s.total_vacancies,
    s.avg_salary,
    s.data_source,
    s.created_at
FROM job_market_nl.it_market_snapshot s;

-- Grant read access (adjust as needed for your setup)
-- GRANT SELECT ON ALL TABLES IN SCHEMA job_market_nl TO superset_user;
