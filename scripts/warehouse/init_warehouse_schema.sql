-- Initialize schema for dashboard-ready job market views

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS odp_staffing_demand;

-- Create denormalized view for BI
CREATE OR REPLACE VIEW odp_staffing_demand.vw_it_market_snapshot_full AS
SELECT
    s.snapshot_date,
    s.total_vacancies,
    s.avg_salary,
    s.data_source,
    s.created_at
FROM odp_staffing_demand.it_market_snapshot s;

-- Grant read access (adjust as needed for your setup)
-- GRANT SELECT ON ALL TABLES IN SCHEMA odp_staffing_demand TO superset_user;
