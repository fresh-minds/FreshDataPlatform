#!/usr/bin/env python3
"""
Superset onboarding script driven by `schema/metrics.yaml`.

Key behavior:
- load metric registry from `schema/metrics.yaml`
- in production mode, only include ACTIVE + CERTIFIED metrics
- create datasets only for the selected metric sources
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

# Superset requires this for the Flask app
os.environ.setdefault("SUPERSET_CONFIG_PATH", "/app/pythonpath/superset_config.py")

try:
    from superset import create_app

    app = create_app()
except ImportError:
    print("Error: This script must be run inside the Superset container")
    sys.exit(1)


# Configuration
SUPERSET_DB_USER = os.getenv("WAREHOUSE_USER", "admin")
SUPERSET_DB_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")
SUPERSET_DB_NAME = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")

WAREHOUSE_CONFIG = {
    "database_name": "Open Data Platform Warehouse",
    "sqlalchemy_uri": f"postgresql+psycopg2://{SUPERSET_DB_USER}:{SUPERSET_DB_PASSWORD}@warehouse:5432/{SUPERSET_DB_NAME}",
    "extra": '{"allows_virtual_table_explore": true}',
}

SUPERSET_ENV = os.getenv("SUPERSET_ENV", "development").strip().lower()
METRICS_REGISTRY_PATH = os.getenv("METRICS_REGISTRY_PATH", "").strip()


def resolve_metrics_registry_path() -> Path:
    """Resolve metrics registry path from env or common project locations."""
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parents[1]
    candidates = []

    if METRICS_REGISTRY_PATH:
        candidates.append(Path(METRICS_REGISTRY_PATH))

    candidates.extend(
        [
            repo_root / "schema" / "metrics.yaml",
            Path("/app/schema/metrics.yaml"),
            Path("/opt/airflow/project/schema/metrics.yaml"),
        ]
    )

    for candidate in candidates:
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not find metrics registry. "
        "Set METRICS_REGISTRY_PATH or mount schema/metrics.yaml into the container."
    )


def load_metrics_registry(metrics_path: Path) -> List[Dict[str, Any]]:
    """Load metrics registry YAML and return metrics list."""
    with metrics_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    metrics = payload.get("metrics", [])
    if not isinstance(metrics, list) or not metrics:
        raise ValueError(f"No metrics found in {metrics_path}")
    return metrics


def select_metrics(metrics: List[Dict[str, Any]], environment: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Select metrics for onboarding based on environment."""
    active = [m for m in metrics if str(m.get("status", "ACTIVE")).upper() == "ACTIVE"]

    if environment == "production":
        selected = [m for m in active if str(m.get("certification", "")).upper() == "CERTIFIED"]
        skipped = [m for m in active if str(m.get("certification", "")).upper() != "CERTIFIED"]
        return selected, skipped

    return active, []


def build_dataset_configs(metrics: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Build unique dataset configs from metric `source.dataset` entries.

    Expects dataset names in `schema.table` format.
    """
    unique: set[tuple[str, str]] = set()

    for metric in metrics:
        source = metric.get("source", {}) or {}
        dataset = source.get("dataset")
        if not isinstance(dataset, str):
            raise ValueError(f"Metric `{metric.get('id', 'unknown')}` is missing `source.dataset`")

        parts = dataset.split(".")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(
                f"Metric `{metric.get('id', 'unknown')}` has invalid dataset `{dataset}`. "
                "Expected `schema.table`."
            )

        unique.add((parts[0], parts[1]))

    return [
        {"schema": schema_name, "table_name": table_name}
        for schema_name, table_name in sorted(unique)
    ]


def create_database_connection():
    """Create or update the database connection to the warehouse."""
    with app.app_context():
        from superset.extensions import db
        from superset.models.core import Database

        existing = db.session.query(Database).filter_by(
            database_name=WAREHOUSE_CONFIG["database_name"]
        ).first()

        if existing:
            print(
                f"✓ Database connection '{WAREHOUSE_CONFIG['database_name']}' "
                f"already exists (id={existing.id})"
            )
            return existing

        database = Database(
            database_name=WAREHOUSE_CONFIG["database_name"],
            sqlalchemy_uri=WAREHOUSE_CONFIG["sqlalchemy_uri"],
            extra=WAREHOUSE_CONFIG["extra"],
        )

        db.session.add(database)
        db.session.commit()

        print(f"✓ Created database connection: {WAREHOUSE_CONFIG['database_name']} (id={database.id})")
        return database


def create_datasets(database, datasets: List[Dict[str, str]]) -> None:
    """Register datasets in Superset."""
    with app.app_context():
        from superset.connectors.sqla.models import SqlaTable
        from superset.extensions import db

        for ds_config in datasets:
            table_name = ds_config["table_name"]
            schema_name = ds_config["schema"]

            existing = db.session.query(SqlaTable).filter_by(
                table_name=table_name,
                schema=schema_name,
                database_id=database.id,
            ).first()

            if existing:
                print(f"  - Dataset '{schema_name}.{table_name}' already exists")
                continue

            try:
                dataset = SqlaTable(
                    table_name=table_name,
                    schema=schema_name,
                    database_id=database.id,
                )
                db.session.add(dataset)
                db.session.commit()

                dataset.fetch_metadata()
                db.session.commit()

                print(f"  ✓ Created dataset: {schema_name}.{table_name}")
            except Exception as exc:
                print(f"  ✗ Error creating dataset {schema_name}.{table_name}: {exc}")
                db.session.rollback()


def test_connection(database) -> bool:
    """Test the warehouse connection."""
    with app.app_context():
        try:
            engine = database.get_sqla_engine()
            with engine.connect() as conn:
                from sqlalchemy import text

                result = conn.execute(text("SELECT 1")).scalar()
                if result == 1:
                    print("✓ Database connection test: PASSED")
                    return True
        except Exception as exc:
            print(f"✗ Database connection test failed: {exc}")
            return False
    return False


def main() -> None:
    print("\n" + "=" * 60)
    print("Superset Onboarding from Metrics Registry")
    print("=" * 60 + "\n")

    print(f"Environment mode: {SUPERSET_ENV}")

    metrics_path = resolve_metrics_registry_path()
    print(f"Using metrics registry: {metrics_path}")
    all_metrics = load_metrics_registry(metrics_path)
    selected_metrics, skipped_metrics = select_metrics(all_metrics, SUPERSET_ENV)

    print(f"Total metrics in registry: {len(all_metrics)}")
    print(f"Selected for onboarding: {len(selected_metrics)}")

    if SUPERSET_ENV == "production":
        print("Production mode: only CERTIFIED + ACTIVE metrics are allowed.")
        if skipped_metrics:
            skipped_ids = ", ".join(str(m.get("id", "unknown")) for m in skipped_metrics)
            print(f"Skipped non-certified ACTIVE metrics: {skipped_ids}")
        if not selected_metrics:
            print("No certified active metrics found; skipping production onboarding.")
            return

    selected_ids = ", ".join(str(m.get("id", "unknown")) for m in selected_metrics)
    print(f"Metric IDs onboarded: {selected_ids}")

    datasets = build_dataset_configs(selected_metrics)
    print(f"Datasets derived from selected metrics: {len(datasets)}")

    print("\n1. Setting up database connection...")
    database = create_database_connection()

    print("\n2. Testing database connection...")
    if not test_connection(database):
        print("\nWarning: Warehouse connection failed; dataset creation may not complete.")

    print("\n3. Creating datasets from selected metrics...")
    create_datasets(database, datasets)

    print("\n" + "=" * 60)
    print("Onboarding complete")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Open Superset and verify datasets in Data -> Datasets")
    print("2. Build production dashboards only from certified metrics")
    print("3. Promote DRAFT metrics in schema/metrics.yaml before production use")
    print()


if __name__ == "__main__":
    main()
