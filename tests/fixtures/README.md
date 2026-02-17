# E2E Fixture Data

Deterministic fixture data for end-to-end platform testing.

- `generate_e2e_fixtures.py`: CLI generator for all fixture files.
- `generated/`: source-side fixture CSVs (bronze-like inputs per domain).
- `golden/`: expected outputs and metrics used by assertions.

Regenerate fixtures:

```bash
.venv/bin/python tests/fixtures/generate_e2e_fixtures.py
```
