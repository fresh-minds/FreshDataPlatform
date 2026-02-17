# Data Modeling Schema Directory

This directory contains all data modeling artifacts for the Open Data Platform, following schema-as-code principles.

## Structure

```
schema/
├── README.md           # This file
├── standards.md        # Modeling conventions and quality rules
├── glossary.yaml       # Business term definitions (conceptual layer)
├── metrics.yaml        # Canonical metric definitions (semantic governance)
├── data_quality_rules.yaml # Centralized data quality rules (single definition source)
├── warehouse.dbml      # Physical warehouse schema (auto-generated)
└── domains/            # Domain-specific logical models (optional)
    └── template_product.yaml # Template for domain data product governance metadata
```

## DBML (Database Markup Language)

We use [DBML](https://dbml.dbdiagram.io/home/) for defining table structures, relationships, and documentation.

### Quick Syntax Reference

```dbml
Table fact_requests {
  id varchar [pk]
  category varchar [note: 'Request category']
  organization varchar [note: 'Requesting organization']
  date_received timestamp
  unit_id varchar [ref: > dim_unit.id]
}

Table dim_unit {
  id varchar [pk]
  name varchar
}
```

### Visualizing

- Use [dbdiagram.io](https://dbdiagram.io/) to visualize DBML files
- Or install `@dbml/cli` for local tooling

## Glossary

The `glossary.yaml` file contains business term definitions that map to DataHub Glossary Terms.

## Standards and Validation

Modeling rules are documented in `schema/standards.md`.

Run schema validation locally before opening a PR:

```bash
make schema-validate
make governance-validate
make dq-list
```

Check for physical schema drift (requires warehouse access):

```bash
make schema-drift-check
```

## Syncing to DataHub

Run the sync script to push schema definitions to DataHub:

```bash
python scripts/sync_dbml_to_datahub.py
```

## Generating from Warehouse

To regenerate DBML from the current PostgreSQL warehouse:

```bash
python scripts/introspect_warehouse.py \
  --schemas finance,dbt_parallel_finance,dbt_parallel_finance_silver,job_market_nl,snapshots,public
```
