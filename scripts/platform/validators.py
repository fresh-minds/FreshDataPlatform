"""Validation logic for the model-driven platform.

Reads all entity and dimension YAMLs from ``schema/models/`` and checks that
the physical artefacts (dbt models, DAGs, DDL, dataset configs) are consistent
with the logical model definitions.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _find_yamls(directory: Path, glob_pattern: str = "*.yaml") -> List[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(glob_pattern))


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_domain_yaml(domain_dir: Path) -> List[str]:
    """Verify domain.yaml exists and has required fields."""
    errors: List[str] = []
    domain_path = domain_dir / "domain.yaml"
    if not domain_path.exists():
        errors.append(f"Missing domain.yaml in {domain_dir.relative_to(REPO_ROOT)}")
        return errors

    data = _load_yaml(domain_path)
    domain = data.get("domain", {})
    for field in ("name", "owner", "classification"):
        if not domain.get(field):
            errors.append(f"{domain_path.relative_to(REPO_ROOT)}: missing domain.{field}")

    return errors


def _check_entity_has_dbt_source(domain: str, entity: Dict, entity_path: Path) -> List[str]:
    """Verify the entity's bronze source table is declared in dbt sources YAML."""
    errors: List[str] = []
    layer_mapping = entity.get("layer_mapping", {})
    bronze_source = layer_mapping.get("bronze_source")
    if not bronze_source:
        return errors  # No bronze source mapping — might be aggregate-only

    sources_yml = REPO_ROOT / "dbt" / "models" / "bronze" / domain / f"_{domain}__sources.yml"
    if not sources_yml.exists():
        errors.append(
            f"{entity_path.relative_to(REPO_ROOT)}: dbt source YAML missing at "
            f"dbt/models/bronze/{domain}/_{domain}__sources.yml"
        )
        return errors

    sources_data = _load_yaml(sources_yml)
    declared_tables = []
    for source in sources_data.get("sources", []):
        for table in source.get("tables", []):
            declared_tables.append(table.get("name"))

    if bronze_source not in declared_tables:
        errors.append(
            f"{entity_path.relative_to(REPO_ROOT)}: bronze_source '{bronze_source}' "
            f"not found in dbt source YAML (declared tables: {declared_tables})"
        )

    return errors


def _check_entity_has_dag(entity: Dict, entity_path: Path) -> List[str]:
    """Verify at least one source references a DAG that exists."""
    errors: List[str] = []
    sources = entity.get("sources", [])
    dags_dir = REPO_ROOT / "dags"

    for source in sources:
        dag_id = source.get("dag")
        if not dag_id:
            continue
        # Look for a DAG file that contains the DAG ID
        dag_file = dags_dir / f"{dag_id.replace('_pipeline', '_dag')}.py"
        # Also try the exact name
        dag_file_alt = dags_dir / f"{dag_id}.py"

        found = False
        if dag_file.exists() or dag_file_alt.exists():
            found = True
        else:
            # Search all DAG files for the DAG ID string
            for py_file in dags_dir.glob("*.py"):
                if dag_id in py_file.read_text():
                    found = True
                    break

        if not found:
            errors.append(
                f"{entity_path.relative_to(REPO_ROOT)}: DAG '{dag_id}' "
                f"not found in any file under dags/"
            )

    return errors


def _check_conformed_dimensions(entity: Dict, entity_path: Path) -> List[str]:
    """Verify every conformed_dimension reference resolves to a YAML definition."""
    errors: List[str] = []
    dims_dir = REPO_ROOT / "schema" / "models" / "_shared" / "dimensions"

    for attr in entity.get("attributes", []):
        cd = attr.get("conformed_dimension")
        if not cd:
            continue
        dim_yaml = dims_dir / f"{cd}.yaml"
        if not dim_yaml.exists():
            errors.append(
                f"{entity_path.relative_to(REPO_ROOT)}: conformed_dimension '{cd}' "
                f"has no YAML definition at schema/models/_shared/dimensions/{cd}.yaml"
            )

    return errors


def _check_entity_has_dataset_config(domain: str, entity: Dict, entity_path: Path) -> List[str]:
    """Verify a governance dataset config exists."""
    errors: List[str] = []
    layer_mapping = entity.get("layer_mapping", {})
    bronze_source = layer_mapping.get("bronze_source", "")
    entity_name = entity.get("entity", "")

    configs_dir = REPO_ROOT / "tests" / "configs" / "datasets"
    if not configs_dir.exists():
        return errors

    # Build a list of candidate file names.  Existing dataset configs may use
    # shortened domain prefixes (e.g. "job_market_" instead of "job_market_nl_")
    # so we also try common prefix variants.
    domain_prefixes = [domain]
    # Add a shorter prefix by stripping trailing locale codes like _nl, _de, _us
    if "_" in domain:
        parts = domain.rsplit("_", 1)
        if len(parts[1]) <= 3:  # likely a locale suffix
            domain_prefixes.append(parts[0])

    expected_patterns = []
    for prefix in domain_prefixes:
        if bronze_source:
            expected_patterns.append(f"{prefix}_{bronze_source}.yml")
        if entity_name:
            expected_patterns.append(f"{prefix}_{entity_name}.yml")

    found = any((configs_dir / p).exists() for p in expected_patterns)

    # Also check by scanning existing config files for a matching dataset ID
    if not found:
        target_dataset_ids = set()
        if bronze_source:
            target_dataset_ids.add(f"{domain}.{bronze_source}")
        for cfg_file in configs_dir.glob("*.yml"):
            try:
                cfg_data = _load_yaml(cfg_file)
                dataset_id = cfg_data.get("dataset", "")
                if dataset_id in target_dataset_ids:
                    found = True
                    break
            except Exception:
                continue

    # Also check by any file prefixed with any of our domain prefixes
    if not found:
        for prefix in domain_prefixes:
            for cfg_file in configs_dir.glob(f"{prefix}_*.yml"):
                found = True
                break
            if found:
                break

    # This is a soft check — warn rather than error
    if not found:
        errors.append(
            f"{entity_path.relative_to(REPO_ROOT)}: no dataset config found in "
            f"tests/configs/datasets/ for {domain}.{bronze_source} (optional but recommended)"
        )

    return errors


def _check_cross_domain_yaml() -> List[str]:
    """Verify cross-domain relationship references resolve."""
    errors: List[str] = []
    cross_domain_path = REPO_ROOT / "schema" / "models" / "_shared" / "cross_domain.yaml"
    if not cross_domain_path.exists():
        return errors

    data = _load_yaml(cross_domain_path)
    relationships = data.get("cross_domain_relationships", [])

    for rel in relationships:
        for ref in (rel.get("from", ""), rel.get("to", "")):
            if not ref or "." not in ref:
                continue
            domain, entity = ref.split(".", 1)
            entity_yaml = REPO_ROOT / "schema" / "models" / domain / "entities" / f"{entity}.yaml"
            if not entity_yaml.exists():
                errors.append(
                    f"cross_domain.yaml: relationship '{rel.get('name')}' references "
                    f"'{ref}' but {entity_yaml.relative_to(REPO_ROOT)} does not exist"
                )

    return errors


# ---------------------------------------------------------------------------
# Main validate
# ---------------------------------------------------------------------------

def validate() -> Tuple[int, int]:
    """
    Run all validation checks against the model registry.

    Returns (error_count, warning_count).
    """
    print("\n=== Platform Model Validation ===\n")

    errors: List[str] = []
    warnings: List[str] = []

    models_dir = REPO_ROOT / "schema" / "models"
    if not models_dir.exists():
        print("  No schema/models/ directory found — nothing to validate.\n")
        return 0, 0

    # Discover domains (skip _shared)
    domain_dirs = [
        d for d in sorted(models_dir.iterdir())
        if d.is_dir() and not d.name.startswith("_")
    ]

    for domain_dir in domain_dirs:
        domain = domain_dir.name
        print(f"  Validating domain: {domain}")

        # Check domain.yaml
        errors.extend(_check_domain_yaml(domain_dir))

        # Check each entity
        entities_dir = domain_dir / "entities"
        if not entities_dir.exists():
            warnings.append(f"  {domain}: no entities/ directory")
            continue

        for entity_path in _find_yamls(entities_dir):
            entity = _load_yaml(entity_path)
            entity_name = entity_path.stem
            print(f"    Checking entity: {entity_name}")

            errors.extend(_check_entity_has_dbt_source(domain, entity, entity_path))
            errors.extend(_check_entity_has_dag(entity, entity_path))
            errors.extend(_check_conformed_dimensions(entity, entity_path))
            warnings.extend(_check_entity_has_dataset_config(domain, entity, entity_path))

    # Check cross-domain relationships
    errors.extend(_check_cross_domain_yaml())

    # Check shared dimensions have dbt models
    shared_dims_dir = REPO_ROOT / "schema" / "models" / "_shared" / "dimensions"
    if shared_dims_dir.exists():
        for dim_yaml in _find_yamls(shared_dims_dir):
            dim_data = _load_yaml(dim_yaml)
            dim_name = dim_data.get("dimension", dim_yaml.stem)
            dbt_model = REPO_ROOT / "dbt" / "models" / "gold" / "_shared" / f"{dim_name}.sql"
            if not dbt_model.exists():
                errors.append(
                    f"Shared dimension '{dim_name}' defined in "
                    f"{dim_yaml.relative_to(REPO_ROOT)} but dbt model not found at "
                    f"dbt/models/gold/_shared/{dim_name}.sql"
                )

    # Report results
    print()
    if warnings:
        print(f"  WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"    ⚠ {w}")

    if errors:
        print(f"\n  ERRORS ({len(errors)}):")
        for e in errors:
            print(f"    ✗ {e}")
        print(f"\n  Validation FAILED: {len(errors)} error(s), {len(warnings)} warning(s)\n")
    else:
        print(f"  Validation PASSED: 0 errors, {len(warnings)} warning(s)\n")

    return len(errors), len(warnings)
