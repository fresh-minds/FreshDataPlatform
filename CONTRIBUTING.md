# Contributing Guide

This project welcomes forks and external contributions.

## Summary

Before opening a pull request:

1. Run required quality checks.
2. Sign commits with DCO (`Signed-off-by`).
3. Check third-party runtime license risk when dependencies or images change.

## Prerequisites

- Git configured with your real name and email:

```bash
git config user.name
git config user.email
```

- Project dependencies installed:

```bash
make dev-install
```

## Standard Verification Steps

Run these before PR:

```bash
make lint
make test
make schema-validate
```

For platform-impacting changes, also run:

```bash
make qa-test
make test-e2e
```

## DCO Sign-Off Requirement

All commits must include a DCO sign-off line.

Create signed commits:

```bash
git commit -s -m "Your commit message"
```

If you forgot to sign:

```bash
git commit --amend -s --no-edit
```

Verification:

```bash
git log -1 --pretty=%B
```

Expected: commit message includes a `Signed-off-by:` line matching your identity.

## Third-Party License And Legal Guardrail

Your code in this repository is MIT licensed, but runtime components can carry different licenses.

- Review [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
- Run license triage when you add/change Docker images or major dependencies:

```bash
make license-risk-check
```

To fail checks when high-risk license families are present:

```bash
FAIL_ON_RESTRICTIVE=true ./scripts/quality/check_license_risk.sh docker-compose.yml
```

Constraints:

- Do not add or upgrade runtime images without checking their upstream license terms.
- If a change introduces a new high-risk family (for example AGPL/GPL/source-available), document it in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) in the same PR.

## Pull Request Checklist

- [ ] Tests/checks completed.
- [ ] Commit(s) are DCO signed (`Signed-off-by` present).
- [ ] `THIRD_PARTY_LICENSES.md` updated if third-party runtime licensing changed.
- [ ] Documentation updated for any behavior/config/security changes.
