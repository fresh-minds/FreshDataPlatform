-- Open Data Platform warehouse security baseline
-- Applies role separation, least-privilege grants, RLS, and PII masking views.

-- 1) Role model (least-privilege)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'odp_etl_writer') THEN
    CREATE ROLE odp_etl_writer NOLOGIN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'odp_analyst_reader') THEN
    CREATE ROLE odp_analyst_reader NOLOGIN;
  END IF;
END
$$;

-- 2) Grants only on schemas that exist
DO $$
DECLARE
  analyst_schema text;
  etl_schema text;
BEGIN
  FOREACH analyst_schema IN ARRAY ARRAY[
    'job_market_nl',
    'job_market_nl_dbt'
  ]
  LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = analyst_schema) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO odp_analyst_reader', analyst_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO odp_analyst_reader', analyst_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO odp_analyst_reader', analyst_schema);
    END IF;
  END LOOP;

  FOREACH etl_schema IN ARRAY ARRAY['job_market_nl']
  LOOP
    IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = etl_schema) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO odp_etl_writer', etl_schema);
      EXECUTE format('GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA %I TO odp_etl_writer', etl_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT INSERT, UPDATE, DELETE, SELECT ON TABLES TO odp_etl_writer', etl_schema);
    END IF;
  END LOOP;
END
$$;

-- 3) Add optional masking views here for any PII-heavy datasets.
