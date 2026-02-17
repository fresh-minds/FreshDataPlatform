# Open Data Platform Schema Standards (DBML)

This document defines the minimum quality bar for schema-as-code in `schema/`.

## Scope

- Structural/documentation rules below apply to DBML files in `schema/domains/*.dbml`.
- `schema/warehouse.dbml` is an auto-generated physical baseline and is validated via drift detection (`make schema-drift-check`), not business-note completeness rules.

## Rules

### 1) Table names are unique and use snake_case

Use a clear semantic prefix where possible (`fact_`, `dim_`, `bridge_`).

Valid:

```dbml
Table fact_transactions {
  pk_grootboek varchar [pk, note: 'Surrogate key']
  Note: 'Financial transaction fact table'
}
```

Invalid:

```dbml
Table FactTransactions {
  id varchar [pk]
}
```

### 2) Every table has a primary key

At least one column must be marked with `[pk]`.

Valid:

```dbml
Table dim_unit {
  id varchar [pk, note: 'Business unit identifier']
  name varchar [not null, note: 'Business unit name']
  Note: 'Business unit dimension'
}
```

Invalid:

```dbml
Table dim_unit {
  id varchar
  name varchar
  Note: 'Business unit dimension'
}
```

### 3) Every table has a table-level note

Each table must include `Note: '...'` inside the table block.

Valid:

```dbml
Table dim_periode {
  id varchar [pk, note: 'Period key']
  Note: 'Financial period dimension'
}
```

Invalid:

```dbml
Table dim_periode {
  id varchar [pk, note: 'Period key']
}
```

### 4) Key business fields include column notes

Key business fields must include `note: '...'` in column attributes.

`Key business field` means any non-technical, non-identifier field:
- not a PK
- not named `id`
- not ending in `id` or `_id`
- not starting with `_` (technical metadata fields)

Valid:

```dbml
Table fact_timelogs {
  TimelogID varchar [pk, note: 'Unique timelog identifier']
  Hours decimal [not null, note: 'Hours worked']
  Description text [note: 'Work description']
  Note: 'Fact table containing time logs'
}
```

Invalid:

```dbml
Table fact_timelogs {
  TimelogID varchar [pk, note: 'Unique timelog identifier']
  Hours decimal [not null]
  Note: 'Fact table containing time logs'
}
```

### 5) `Ref` targets must resolve to real tables/columns

References with `>` must point to known tables and columns in the validated DBML set.

Valid:

```dbml
Table fact_transactions {
  GrootboekrekeningID varchar [ref: > dim_grootboek.id]
  Note: 'Financial transaction fact table'
}

Table dim_grootboek {
  id varchar [pk, note: 'Ledger key']
  Note: 'General ledger dimension'
}
```

Invalid:

```dbml
Table fact_transactions {
  GrootboekrekeningID varchar [ref: > dim_grootboek.missing_column]
  Note: 'Financial transaction fact table'
}
```

## Change Management

- Treat DBML changes as code changes: PR review is required.
- Run `make schema-validate` before opening a PR.
- Keep notes and references in sync with DataHub metadata publishing.

### 6) PII and Confidentiality Tagging

All fields containing Personally Identifiable Information (PII) must be tagged.

Valid:

```dbml
Table dim_medewerker {
  bsn varchar [note: 'Burger Service Nummer', pii: true, confidentiality: 'high']
}
```
