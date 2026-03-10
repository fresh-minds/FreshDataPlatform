#!/usr/bin/env python
"""Model-driven platform CLI.

Usage
-----
  python scripts/platform/cli.py init-domain <domain>
  python scripts/platform/cli.py add-entity  <domain> <entity>
  python scripts/platform/cli.py validate
  python scripts/platform/cli.py sync

Commands
--------
  init-domain   Create the full directory scaffold for a new domain
  add-entity    Generate dbt, DAG, and DDL scaffolds from an entity YAML
  validate      Check that all model YAMLs are consistent with physical artefacts
  sync          Regenerate derived config files from entity YAMLs (non-destructive)
"""

from __future__ import annotations

import argparse
import os
import sys

# Ensure repo root is on sys.path so `scripts.platform.*` imports resolve
# regardless of how the script is invoked.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="platform",
        description="Model-driven data platform CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- init-domain ---
    p_init = sub.add_parser("init-domain", help="Create scaffold for a new domain")
    p_init.add_argument("domain", help="Domain name (snake_case, e.g. finance)")

    # --- add-entity ---
    p_add = sub.add_parser("add-entity", help="Generate artefacts from an entity YAML")
    p_add.add_argument("domain", help="Domain name")
    p_add.add_argument("entity", help="Entity name (must match schema/models/<domain>/entities/<entity>.yaml)")

    # --- validate ---
    sub.add_parser("validate", help="Validate model registry against physical artefacts")

    # --- sync ---
    sub.add_parser("sync", help="Regenerate derived configs from entity YAMLs")

    args = parser.parse_args(argv)

    if args.command == "init-domain":
        from scripts.platform.generators import init_domain

        init_domain(args.domain)
        return 0

    elif args.command == "add-entity":
        from scripts.platform.generators import add_entity

        add_entity(args.domain, args.entity)
        return 0

    elif args.command == "validate":
        from scripts.platform.validators import validate

        error_count, _ = validate()
        return 1 if error_count > 0 else 0

    elif args.command == "sync":
        # Sync iterates over all domains and entities and re-runs add-entity
        # in non-overwrite mode (only writes missing files)
        from pathlib import Path

        from scripts.platform.generators import REPO_ROOT, add_entity

        models_dir = REPO_ROOT / "schema" / "models"
        if not models_dir.exists():
            print("No schema/models/ directory found — nothing to sync.")
            return 0

        for domain_dir in sorted(models_dir.iterdir()):
            if not domain_dir.is_dir() or domain_dir.name.startswith("_"):
                continue
            entities_dir = domain_dir / "entities"
            if not entities_dir.exists():
                continue
            for entity_yaml in sorted(entities_dir.glob("*.yaml")):
                add_entity(domain_dir.name, entity_yaml.stem)

        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
