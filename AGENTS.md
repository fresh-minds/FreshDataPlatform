# Agent Instructions: Documentation Discipline

These instructions apply to every change in this repository.

## Core Rule
- Any code change that affects behavior, interfaces, setup, operations, security, testing, or outputs must include documentation updates in the same change.

## Changes That Require Documentation Updates
- New, removed, renamed, or behavior-changed scripts, commands, or Make targets.
- API, pipeline, DAG, dbt, schema, or data contract changes.
- Environment variable, config, secret, default value, or runtime dependency changes.
- Deployment, infrastructure, or platform workflow changes.
- Authentication, authorization, or security-related changes.
- Test workflow or verification procedure changes.

## Documentation Files To Check
- `README.md` for user-facing setup, usage, and key workflows.
- `DEVELOPMENT.md` for local developer workflows and commands.
- `DEPLOYMENT.md` for deployment and runtime operations.
- `ARCHITECTURE.md` for component boundaries and interactions.
- `DATA_MODEL.md` and `schema/*` for data model and contract changes.
- `SECURITY.md` and `GIT_SECURITY_CHECKLIST.md` for security-impacting updates.
- `docs/*` and `guides/*` for topic-specific workflows.
- `.env.template` whenever env vars are added/changed/removed.

## Required Documentation Structure
- Start with a short summary of what changed.
- Provide exact commands/config snippets (copy-pasteable).
- State prerequisites and constraints.
- Include clear verification steps.
- Keep section headings stable and easy to scan.
- Remove or update stale references in the same change.

## Completion Checklist (Must Pass Before Finalizing)
- [ ] All impacted docs were updated.
- [ ] Stale or conflicting instructions were removed.
- [ ] Examples and commands match current file paths and behavior.
- [ ] Env var changes are reflected in `.env.template` and relevant docs.
- [ ] Final change summary lists which docs were updated and why.

## If Documentation Updates Are Not Needed
- Explicitly state why no docs changed.
- List which documentation files were reviewed.

## Default Decision Rule
- If uncertain whether docs are needed, update them.

## Nested AGENTS Strategy
- Use local instructions in these subprojects when working there:
  - `frontend/AGENTS.md`
  - `dbt_parallel/AGENTS.md`
  - `scripts/AGENTS.md`
  - `schema/AGENTS.md`
  - `k8s/AGENTS.md`
  - `deploy/AGENTS.md`
  - `ops/minio-sso-bridge/AGENTS.md`
- All other directories inherit this root policy.
