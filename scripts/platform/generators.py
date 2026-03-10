"""Code generation logic for the model-driven platform.

Reads entity and domain YAML definitions from ``schema/models/`` and generates
scaffold files for dbt, Airflow DAGs, Postgres DDL, and dataset configs.
"""

from __future__ import annotations

import os
import textwrap
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> Dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _write_file(path: Path, content: str, *, overwrite: bool = False) -> bool:
    """Write *content* to *path*.  Return True if written, False if skipped."""
    if path.exists() and not overwrite:
        print(f"  SKIP (exists) {path.relative_to(REPO_ROOT)}")
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"  WROTE {path.relative_to(REPO_ROOT)}")
    return True


# ---------------------------------------------------------------------------
# init-domain
# ---------------------------------------------------------------------------

def init_domain(domain: str) -> None:
    """Create the full directory scaffold for a new domain."""
    print(f"\n=== Initialising domain: {domain} ===\n")

    # schema/models/<domain>/
    models_dir = REPO_ROOT / "schema" / "models" / domain
    entities_dir = models_dir / "entities"
    entities_dir.mkdir(parents=True, exist_ok=True)

    _write_file(
        models_dir / "domain.yaml",
        textwrap.dedent(f"""\
        domain:
          name: {domain}
          display_name: "{domain.replace('_', ' ').title()}"
          description: "TODO: describe the {domain} domain"
          owner: "{domain}.owner@example.com"
          steward: "{domain}.steward@example.com"
          classification: internal
          criticality: medium

        entities: []

        relationships: []
        """),
    )

    # schema/domains/<domain>_product.yaml
    _write_file(
        REPO_ROOT / "schema" / "domains" / f"{domain}_product.yaml",
        textwrap.dedent(f"""\
        version: 1
        product:
          name: "{domain.replace('_', ' ').title()}"
          owner: "{domain}.owner@example.com"
          steward: "{domain}.steward@example.com"
          description: "TODO: describe the {domain} data product."
          classification: internal
          sla:
            freshness_hours: 24
            availability_percent: 99.0
          datasets: []
        """),
    )

    # pipelines/<domain>/
    pipelines_dir = REPO_ROOT / "pipelines" / domain
    pipelines_dir.mkdir(parents=True, exist_ok=True)
    _write_file(pipelines_dir / "__init__.py", "")
    _write_file(
        pipelines_dir / "postgres_pipeline.py",
        textwrap.dedent(f'''\
        """Postgres warehouse tables for the {domain} domain.

        Import ``ensure_tables`` and call it with a psycopg2 connection to
        create all source tables for this domain.  New tables are added by
        the ``platform add-entity`` CLI when an entity YAML is defined in
        ``schema/models/{domain}/entities/``.
        """

        from __future__ import annotations


        def ensure_tables(conn) -> None:
            """Create all {domain} source tables if they don\'t already exist."""
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS {domain}")
                # --- Tables below are auto-managed by `platform sync` ---
            conn.commit()
        '''),
    )

    # dbt model directories
    for layer in ("bronze", "silver", "gold"):
        dbt_dir = REPO_ROOT / "dbt" / "models" / layer / domain
        dbt_dir.mkdir(parents=True, exist_ok=True)

    _write_file(
        REPO_ROOT / "dbt" / "models" / "bronze" / domain / f"_{domain}__sources.yml",
        textwrap.dedent(f"""\
        version: 2

        sources:
          - name: {domain}
            schema: {domain}
            description: "Source tables for the {domain} domain."
            tables: []
        """),
    )

    _write_file(
        REPO_ROOT / "dbt" / "models" / "bronze" / domain / f"_brz_{domain}__models.yml",
        textwrap.dedent("""\
        version: 2

        models: []
        """),
    )

    _write_file(
        REPO_ROOT / "dbt" / "models" / "silver" / domain / f"_slv_{domain}__models.yml",
        textwrap.dedent("""\
        version: 2

        models: []
        """),
    )

    _write_file(
        REPO_ROOT / "dbt" / "models" / "gold" / domain / f"_gold_{domain}__models.yml",
        textwrap.dedent("""\
        version: 2

        models: []
        """),
    )

    print(f"\n  Domain '{domain}' scaffolded.  Next steps:")
    print(f"  1. Edit schema/models/{domain}/domain.yaml with entities & relationships")
    print(f"  2. Create entity YAMLs in schema/models/{domain}/entities/")
    print(f"  3. Run: python scripts/platform/cli.py add-entity {domain} <entity>\n")


# ---------------------------------------------------------------------------
# add-entity
# ---------------------------------------------------------------------------

def _pg_type(yaml_type: str) -> str:
    """Map entity YAML type names to Postgres DDL types."""
    mapping = {
        "text": "TEXT",
        "integer": "INTEGER",
        "int": "INTEGER",
        "double_precision": "DOUBLE PRECISION",
        "double": "DOUBLE PRECISION",
        "float": "DOUBLE PRECISION",
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamptz": "TIMESTAMPTZ",
        "timestamp": "TIMESTAMP",
        "numeric": "NUMERIC",
        "jsonb": "JSONB",
    }
    return mapping.get(yaml_type.lower(), "TEXT")


def add_entity(domain: str, entity_name: str) -> None:
    """Generate scaffold files from an entity YAML definition."""
    entity_path = REPO_ROOT / "schema" / "models" / domain / "entities" / f"{entity_name}.yaml"
    if not entity_path.exists():
        print(f"ERROR: Entity YAML not found at {entity_path.relative_to(REPO_ROOT)}")
        print(f"Create it first, then re-run: python scripts/platform/cli.py add-entity {domain} {entity_name}")
        return

    entity = _load_yaml(entity_path)
    attributes: List[Dict[str, Any]] = entity.get("attributes", [])
    entity_type = entity.get("type", "transactional")
    layer_mapping = entity.get("layer_mapping", {})
    bronze_source = layer_mapping.get("bronze_source", entity_name)

    print(f"\n=== Adding entity: {domain}.{entity_name} ===\n")

    # --- 1. Generate Postgres DDL snippet ---
    _generate_ddl_snippet(domain, entity_name, bronze_source, attributes)

    # --- 2. Generate dbt bronze SQL ---
    _generate_bronze_sql(domain, bronze_source, attributes)

    # --- 3. Generate dbt silver SQL ---
    _generate_silver_sql(domain, bronze_source, entity_name, attributes)

    # --- 4. Generate dbt gold SQL (if transactional → fact) ---
    if entity_type == "transactional":
        gold_model = layer_mapping.get("gold_model", f"fact_{entity_name}")
        gold_dims = layer_mapping.get("gold_dimensions", [])
        _generate_gold_fact_sql(domain, entity_name, bronze_source, gold_model, gold_dims, attributes)

    # --- 5. Generate dataset config ---
    _generate_dataset_config(domain, entity_name, bronze_source, entity)

    print(f"\n  Entity '{domain}.{entity_name}' scaffolded.")
    print("  Review the generated files and add custom business logic as needed.\n")


def _generate_ddl_snippet(domain: str, entity_name: str, table_name: str, attrs: List[Dict]) -> None:
    """Print Postgres CREATE TABLE DDL to stdout (user adds to ensure_tables)."""
    cols = []
    for attr in attrs:
        col = f"              {attr['name']} {_pg_type(attr.get('type', 'text'))}"
        if attr.get("primary_key") or attr.get("not_null"):
            col += " NOT NULL"
        cols.append(col)
    cols.append("              ingestion_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()")

    ddl = (
        f"        cur.execute(\"\"\"\n"
        f"            CREATE TABLE IF NOT EXISTS {domain}.{table_name} (\n"
        + ",\n".join(cols) + "\n"
        f"            )\n"
        f"        \"\"\")\n"
    )

    ddl_path = REPO_ROOT / "pipelines" / domain / f"_ddl_{table_name}.sql.txt"
    _write_file(
        ddl_path,
        f"-- Add this block to pipelines/{domain}/postgres_pipeline.py ensure_tables():\n\n{ddl}",
    )


def _generate_bronze_sql(domain: str, source_table: str, attrs: List[Dict]) -> None:
    """Generate dbt bronze SQL model."""
    select_lines = []
    for attr in attrs:
        name = attr["name"]
        pg_type = _pg_type(attr.get("type", "text")).lower()
        if pg_type in ("text",):
            select_lines.append(f"        trim(cast({name} as text)) as {name}")
        else:
            select_lines.append(f"        cast({name} as {pg_type}) as {name}")
    select_lines.append("        ingested_at")

    pk = next((a["name"] for a in attrs if a.get("primary_key")), None)
    where_clause = f"\n    where {pk} is not null and {pk} <> ''" if pk else ""

    sql = textwrap.dedent(f"""\
    -- Bronze: light type-casting over source table {domain}.{source_table}
    -- Auto-generated by `platform add-entity`. Customise as needed.

    with source as (

        select * from {{{{ source('{domain}', '{source_table}') }}}}

    ),

    bronze as (

        select
{chr(10).join(select_lines)}
        from source{where_clause}

    )

    select * from bronze
    """)

    _write_file(
        REPO_ROOT / "dbt" / "models" / "bronze" / domain / f"brz_{domain}__{source_table}.sql",
        sql,
    )


def _generate_silver_sql(domain: str, source_table: str, entity_name: str, attrs: List[Dict]) -> None:
    """Generate dbt silver SQL model."""
    sql = textwrap.dedent(f"""\
    -- Silver: cleaned and enriched {entity_name} from bronze
    -- Auto-generated by `platform add-entity`. Add joins and business logic.

    with bronze as (

        select * from {{{{ ref('brz_{domain}__{source_table}') }}}}

    ),

    enriched as (

        select
            *
        from bronze

    )

    select * from enriched
    """)

    _write_file(
        REPO_ROOT / "dbt" / "models" / "silver" / domain / f"slv_{domain}__{entity_name}.sql",
        sql,
    )


def _generate_gold_fact_sql(
    domain: str,
    entity_name: str,
    source_table: str,
    gold_model: str,
    gold_dims: List[str],
    attrs: List[Dict],
) -> None:
    """Generate dbt gold fact SQL model."""
    cte_lines = [
        f"with silver as (\n\n    select * from {{{{ ref('slv_{domain}__{entity_name}') }}}}\n\n)",
    ]
    for dim in gold_dims:
        cte_lines.append(
            f"{dim} as (\n\n    select * from {{{{ ref('{dim}') }}}}\n\n)"
        )

    # Build join clauses from conformed_dimension hints
    join_lines = []
    for attr in attrs:
        cd = attr.get("conformed_dimension")
        if cd and cd in gold_dims:
            # Guess the join key based on the dimension name
            if "region" in cd:
                join_lines.append(f"left join {cd} dr on silver.province = dr.region_name")
            elif "company" in cd:
                join_lines.append(f"left join {cd} dc on silver.company = dc.company_name")
            else:
                join_lines.append(f"-- TODO: left join {cd} on silver.??? = {cd}.???")

    pk = next((a["name"] for a in attrs if a.get("primary_key")), "id")
    select_cols = [f"    {{{{ hashed_key(['{pk}']) }}}} as {entity_name}_sk"]
    for attr in attrs:
        select_cols.append(f"    silver.{attr['name']}")
    select_cols.append("    silver.ingested_at")

    joins_str = "\n".join(join_lines) if join_lines else "-- No dimension joins configured"

    sql = textwrap.dedent(f"""\
    -- Gold fact: {gold_model}
    -- Auto-generated by `platform add-entity`. Add dimension FKs and business logic.

    """) + ",\n\n".join(cte_lines) + "\n\nselect\n" + ",\n".join(select_cols) + (
        "\nfrom silver\n" + joins_str + "\n"
    )

    _write_file(
        REPO_ROOT / "dbt" / "models" / "gold" / domain / f"{gold_model}.sql",
        sql,
    )


def _generate_dataset_config(domain: str, entity_name: str, table_name: str, entity: Dict) -> None:
    """Generate a dataset config YAML for governance tests."""
    gov = entity.get("governance", {})
    quality = entity.get("quality", {})
    pk_cols = [a["name"] for a in entity.get("attributes", []) if a.get("primary_key")]

    config = {
        "dataset": f"{domain}.{table_name}",
        "owner": entity.get("owner") or f"{domain}.owner@example.com",
        "description": entity.get("description", f"{entity_name} dataset"),
        "domain": domain,
        "layer": "silver",
        "classification": gov.get("classification", "internal"),
        "sensitivity": gov.get("sensitivity", "normal"),
        "product_tag": domain.replace("_", "-"),
        "pii_columns": gov.get("pii_columns", []),
        "pii_classifications": {},
        "retention_days": gov.get("retention_days", 365),
        "timestamp_column": "ingestion_timestamp",
        "primary_key": pk_cols or [entity_name + "_id"],
        "upstreams": [],
        "tests": {
            "schema": {
                "required_columns": pk_cols + ["ingestion_timestamp"],
            },
            "constraints": {
                "not_null": pk_cols + ["ingestion_timestamp"],
            },
        },
        "governance": {
            "require_rbac": gov.get("require_rbac", False),
            "allowed_roles_read": [],
            "non_authorized_export_queries": [],
        },
    }

    freshness = quality.get("freshness")
    if freshness:
        config["tests"]["freshness"] = {
            "column": freshness.get("column", "ingestion_timestamp"),
            "max_age_hours": freshness.get("max_age_hours", 48),
            "format": "timestamp",
        }

    config_path = REPO_ROOT / "tests" / "configs" / "datasets" / f"{domain}_{table_name}.yml"
    _write_file(config_path, yaml.dump(config, default_flow_style=False, sort_keys=False))
