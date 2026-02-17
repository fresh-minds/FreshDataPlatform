# Open Data Platform Data Governance Operating Model

## Purpose

Define a pragmatic governance model that improves speed, trust, and compliance for data and analytics products built on Open Data Platform.

## Scope

- Domains: job_market_nl.
- Data lifecycle: ingest, transform, serve, retain, archive, delete.
- Analytics scope: BI metrics, semantic models, dashboards, AI extraction and model outputs.

## Principles

- Guardrails over gates: automate policy checks, escalate only high-risk exceptions.
- Federated ownership: domains own data products; platform team provides common standards and controls.
- Metadata by default: no production dataset without owner, steward, SLA, and classification.
- Quality is a product requirement: data quality SLOs are defined and monitored.
- Compliance is engineered: privacy and security controls are embedded in pipelines.

## Roles and Accountability

| Role | Core accountability |
| --- | --- |
| Domain Data Owner | Business accountability for dataset quality, definitions, and access approvals. |
| Domain Data Steward | Metadata quality, glossary alignment, retention and classification maintenance. |
| Data Platform Team | Tooling, policy automation, lineage, CI/CD governance checks, observability. |
| Security and Privacy Lead | PII policy, access model, retention/deletion policy, audit readiness. |
| Analytics Lead | Certified semantic definitions and dashboard lifecycle governance. |
| AI Lead | Model and agent release controls, human-in-the-loop design, monitoring standards. |

## Decision Rights (RACI-lite)

| Decision | Owner | Steward | Platform | Security/Privacy | Governance Council |
| --- | --- | --- | --- | --- | --- |
| Data contract change | A | R | C | I | I |
| Sensitive data classification | C | R | I | A | I |
| Access policy (row/column) | C | R | C | A | I |
| Canonical KPI definition | A | R | C | I | C |
| AI model promotion (regulated/critical) | C | C | R | C | A |

## Governance Control Flow

1. Domain opens change in Git (schema, pipeline, metric, model).
2. CI runs contract, quality, security, metadata, and lineage checks.
3. Low-risk changes merge automatically after checks and owner approval.
4. High-risk changes trigger explicit Security/Privacy or Council review.
5. Production release publishes metadata and monitoring hooks.
6. Failures route to domain ownership with defined SLA and escalation.

## Required Artifacts per Data Product

- Product spec (`*_product.yaml`) with owner, steward, classification, retention, SLAs.
- Metric definitions in `schema/metrics.yaml` where applicable.
- DBML model with notes, keys, refs, and validation in CI.
- Data quality rules and thresholds in pipeline code.
- Access policy and audit trail for sensitive datasets.

## Cadence

- Weekly: domain data triage (quality incidents, SLA breaches, drift).
- Bi-weekly: architecture and schema review across domains.
- Monthly: governance council (policy changes, risk exceptions, KPI conflicts).
- Quarterly: access recertification, retention validation, control evidence review.

## Exception Management

- Exceptions must include owner, justification, risk level, expiration date, and mitigation.
- All exceptions are time-boxed and reviewed monthly.
- Expired exceptions are auto-escalated.

## Framework Translation

- DAMA-DMBOK: ownership, metadata, quality, security, lifecycle controls implemented as operating processes.
- TOGAF governance: architecture decisions and exceptions are explicit, traceable, and reviewable.
- ISO 27001: least privilege, secrets handling, logging, and information lifecycle controls are enforced in run-time and CI.
- EU AI Act readiness: use-case inventory, risk tiering, traceability, oversight, and monitoring for AI workflows.
