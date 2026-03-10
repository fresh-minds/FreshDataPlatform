# PySpark -> dbt Mapping

## ODP Staffing Demand (Phase 2)

- Canonical pipeline imports:
	- `pipelines/odp_staffing_demand/*`
- Canonical dbt shim paths:
	- `models/bronze/odp_staffing_demand/*`
	- `models/silver/odp_staffing_demand/*`
- Current physical implementation path (transitional):
	- `pipelines/odp_staffing_demand/*` -> `models/*/odp_staffing_demand/*`
- Ingestion source template path remains:
	- `models/bronze/*` -> `models/silver/*` -> `models/gold/*`
