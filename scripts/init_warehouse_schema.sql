-- Initialize schema for dashboard-ready finance views

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS finance;

-- Create denormalized view for BI
CREATE OR REPLACE VIEW finance.vw_fact_grootboekmutaties_full AS
SELECT
    f.grootboekmutatieid,
    f.grootboek_id,
    f.grootboekrekening_id,
    f.administratie_code,
    f.grootboekbedrag,
    f.datum_ts,
    f.jaar,
    f.periode,
    f.kostenplaats_code,
    f.kostendrager_code,
    f.relatie_naam,
    f.type
FROM finance.fact_grootboekmutaties f;

-- Grant read access (adjust as needed for your setup)
-- GRANT SELECT ON ALL TABLES IN SCHEMA finance TO superset_user;
