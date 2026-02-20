#!/usr/bin/env python3
"""
Validate DBML schema files used by Open Data Platform.

Checks:
- duplicate table names
- duplicate column names in a table
- missing primary key per table
- missing table note
- missing note on key business fields
- broken Ref targets (for `>` relations, and `-` relations)
"""

from __future__ import annotations

import argparse
import difflib
import glob
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


TABLE_HEADER_RE = re.compile(r"^\s*Table\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{\s*$")
COLUMN_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+([^\s\[]+)(?:\s*\[([^\]]*)\])?\s*$"
)
REF_LINE_RE = re.compile(
    r"^\s*Ref:\s*"
    r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*([<>-])\s*"
    r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*(?://.*)?$"
)
INLINE_REF_RE = re.compile(
    r"ref:\s*([<>-])\s*([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
NOTE_RE = re.compile(r"""note:\s*(['"])(.*?)\1""", re.IGNORECASE)
TABLE_NOTE_RE = re.compile(r"""^\s*Note:\s*(['"])(.*?)\1\s*$""")
GENERATED_AT_RE = re.compile(r"^\s*//\s*Generated at:\s*.+$")


@dataclass
class RefDef:
    left_table: str
    left_column: str
    operator: str
    right_table: str
    right_column: str
    source_file: Path
    line_number: int


@dataclass
class ColumnDef:
    name: str
    data_type: str
    attrs: str
    note: Optional[str]
    is_pk: bool
    line_number: int


@dataclass
class TableDef:
    name: str
    source_file: Path
    start_line: int
    columns: List[ColumnDef] = field(default_factory=list)
    note: Optional[str] = None
    refs: List[RefDef] = field(default_factory=list)

    @property
    def columns_by_name(self) -> Dict[str, ColumnDef]:
        return {column.name: column for column in self.columns}


@dataclass
class ValidationError:
    source_file: Path
    message: str
    table: Optional[str] = None
    column: Optional[str] = None
    line_number: Optional[int] = None

    def format(self, repo_root: Path) -> str:
        try:
            rel = self.source_file.relative_to(repo_root)
        except ValueError:
            rel = self.source_file
        where_parts = []
        if self.table:
            where_parts.append(f"table `{self.table}`")
        if self.column:
            where_parts.append(f"column `{self.column}`")
        if self.line_number is not None:
            where_parts.append(f"line {self.line_number}")
        where = ", ".join(where_parts)
        if where:
            return f"{rel}: {where}: {self.message}"
        return f"{rel}: {self.message}"


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_dbml_files(root: Path, include_warehouse_structure: bool = False) -> List[Path]:
    paths = sorted((root / "schema" / "domains").glob("*.dbml"))
    if include_warehouse_structure:
        paths.append(root / "schema" / "warehouse.dbml")
    return paths


def expand_input_paths(
    paths: Sequence[str],
    root: Path,
    include_warehouse_structure: bool = False,
) -> List[Path]:
    if not paths:
        return default_dbml_files(root, include_warehouse_structure=include_warehouse_structure)

    resolved: List[Path] = []
    for raw in paths:
        pattern = raw
        if not Path(raw).is_absolute():
            pattern = str(root / raw)
        matches = [Path(match) for match in glob.glob(pattern)]
        if matches:
            resolved.extend(matches)
        else:
            resolved.append(Path(pattern))

    # Remove duplicates while preserving order.
    deduped: List[Path] = []
    seen = set()
    for item in resolved:
        key = str(item.resolve()) if item.exists() else str(item)
        if key in seen:
            continue
        deduped.append(item)
        seen.add(key)
    return deduped


def parse_column_note(attrs: str) -> Optional[str]:
    match = NOTE_RE.search(attrs)
    if not match:
        return None
    return match.group(2).strip()


def parse_table(file_path: Path) -> Tuple[List[TableDef], List[RefDef], List[ValidationError]]:
    tables: List[TableDef] = []
    global_refs: List[RefDef] = []
    errors: List[ValidationError] = []

    if not file_path.exists():
        errors.append(ValidationError(file_path, "file does not exist"))
        return tables, global_refs, errors

    lines = file_path.read_text(encoding="utf-8").splitlines()

    current: Optional[TableDef] = None
    for line_number, raw in enumerate(lines, start=1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("//"):
            continue

        if current is None:
            table_header = TABLE_HEADER_RE.match(raw)
            if table_header:
                current = TableDef(
                    name=table_header.group(1),
                    source_file=file_path,
                    start_line=line_number,
                )
                continue

            ref_match = REF_LINE_RE.match(raw)
            if ref_match:
                global_refs.append(
                    RefDef(
                        left_table=ref_match.group(1),
                        left_column=ref_match.group(2),
                        operator=ref_match.group(3),
                        right_table=ref_match.group(4),
                        right_column=ref_match.group(5),
                        source_file=file_path,
                        line_number=line_number,
                    )
                )
            continue

        # Inside table body.
        if stripped == "}":
            tables.append(current)
            current = None
            continue

        table_note = TABLE_NOTE_RE.match(raw)
        if table_note:
            current.note = table_note.group(2).strip()
            continue

        column_match = COLUMN_RE.match(raw)
        if not column_match:
            # Keep parser permissive for advanced DBML constructs we do not validate here.
            continue

        attrs = (column_match.group(3) or "").strip()
        column = ColumnDef(
            name=column_match.group(1),
            data_type=column_match.group(2),
            attrs=attrs,
            note=parse_column_note(attrs),
            is_pk=bool(re.search(r"\bpk\b", attrs, flags=re.IGNORECASE)),
            line_number=line_number,
        )
        current.columns.append(column)

        for inline_ref in INLINE_REF_RE.finditer(attrs):
            current.refs.append(
                RefDef(
                    left_table=current.name,
                    left_column=column.name,
                    operator=inline_ref.group(1),
                    right_table=inline_ref.group(2),
                    right_column=inline_ref.group(3),
                    source_file=file_path,
                    line_number=line_number,
                )
            )

    if current is not None:
        errors.append(
            ValidationError(
                source_file=file_path,
                message="table block is missing closing `}`",
                table=current.name,
                line_number=current.start_line,
            )
        )

    return tables, global_refs, errors


def is_key_business_field(column: ColumnDef) -> bool:
    name = column.name.lower()
    if column.is_pk:
        return False
    if name == "id":
        return False
    if name.startswith("_"):
        return False
    if name.endswith("_id") or name.endswith("id"):
        return False
    return True


def add_error(
    errors: List[ValidationError],
    source_file: Path,
    message: str,
    table: Optional[str] = None,
    column: Optional[str] = None,
    line_number: Optional[int] = None,
) -> None:
    errors.append(
        ValidationError(
            source_file=source_file,
            message=message,
            table=table,
            column=column,
            line_number=line_number,
        )
    )


def validate_ref_target(
    errors: List[ValidationError],
    ref: RefDef,
    target_table: str,
    target_column: str,
    table_registry: Dict[str, TableDef],
) -> None:
    table = table_registry.get(target_table)
    if table is None:
        add_error(
            errors,
            source_file=ref.source_file,
            table=ref.left_table,
            column=ref.left_column,
            line_number=ref.line_number,
            message=f"broken Ref target: table `{target_table}` does not exist",
        )
        return

    if target_column not in table.columns_by_name:
        add_error(
            errors,
            source_file=ref.source_file,
            table=ref.left_table,
            column=ref.left_column,
            line_number=ref.line_number,
            message=(
                f"broken Ref target: column `{target_table}.{target_column}` "
                "does not exist"
            ),
        )


def run_validation(dbml_files: Sequence[Path], strict_left_refs: bool) -> Tuple[List[ValidationError], int, int]:
    errors: List[ValidationError] = []
    table_registry: Dict[str, TableDef] = {}
    parsed_tables: List[TableDef] = []
    all_refs: List[RefDef] = []

    for file_path in dbml_files:
        tables, global_refs, parse_errors = parse_table(file_path)
        errors.extend(parse_errors)
        all_refs.extend(global_refs)

        for table in tables:
            existing = table_registry.get(table.name)
            if existing:
                add_error(
                    errors,
                    source_file=table.source_file,
                    table=table.name,
                    line_number=table.start_line,
                    message=(
                        f"duplicate table name; already defined in "
                        f"{existing.source_file}:{existing.start_line}"
                    ),
                )
            else:
                table_registry[table.name] = table
            parsed_tables.append(table)
            all_refs.extend(table.refs)

    # Table-level and column-level checks.
    for table in parsed_tables:
        if not table.columns:
            add_error(
                errors,
                source_file=table.source_file,
                table=table.name,
                line_number=table.start_line,
                message="table has no columns",
            )
            continue

        if table.note is None or not table.note.strip():
            add_error(
                errors,
                source_file=table.source_file,
                table=table.name,
                line_number=table.start_line,
                message="missing table Note",
            )

        if not any(column.is_pk for column in table.columns):
            add_error(
                errors,
                source_file=table.source_file,
                table=table.name,
                line_number=table.start_line,
                message="missing primary key column ([pk])",
            )

        seen_columns: Dict[str, ColumnDef] = {}
        for column in table.columns:
            previous = seen_columns.get(column.name)
            if previous:
                add_error(
                    errors,
                    source_file=table.source_file,
                    table=table.name,
                    column=column.name,
                    line_number=column.line_number,
                    message=(
                        f"duplicate column; first defined at line {previous.line_number}"
                    ),
                )
            else:
                seen_columns[column.name] = column

            if is_key_business_field(column) and (column.note is None or not column.note.strip()):
                add_error(
                    errors,
                    source_file=table.source_file,
                    table=table.name,
                    column=column.name,
                    line_number=column.line_number,
                    message="missing note on key business field",
                )

    # Ref target checks.
    for ref in all_refs:
        if ref.operator == ">":
            validate_ref_target(errors, ref, ref.right_table, ref.right_column, table_registry)
            continue
        if ref.operator == "-":
            validate_ref_target(errors, ref, ref.right_table, ref.right_column, table_registry)
            validate_ref_target(errors, ref, ref.left_table, ref.left_column, table_registry)
            continue
        if ref.operator == "<" and strict_left_refs:
            validate_ref_target(errors, ref, ref.left_table, ref.left_column, table_registry)

    return errors, len(dbml_files), len(table_registry)


def normalize_dbml_lines(content: str) -> List[str]:
    """Normalize DBML text for stable drift comparisons."""
    lines = [line.rstrip() for line in content.splitlines() if not GENERATED_AT_RE.match(line)]
    while lines and lines[-1] == "":
        lines.pop()
    return lines


def run_warehouse_drift_check(root: Path) -> Tuple[Optional[ValidationError], Optional[str]]:
    """
    Regenerate schema/warehouse.dbml and compare against committed file.

    The dynamic `Generated at` header is ignored during comparison.
    """
    warehouse_dbml = root / "schema" / "warehouse.dbml"
    introspection_script = root / "scripts" / "introspect_warehouse.py"

    if not warehouse_dbml.exists():
        return (
            ValidationError(
                source_file=warehouse_dbml,
                message="warehouse drift check requested but `schema/warehouse.dbml` does not exist",
            ),
            None,
        )

    if not introspection_script.exists():
        return (
            ValidationError(
                source_file=introspection_script,
                message="warehouse drift check requested but introspection script is missing",
            ),
            None,
        )

    with tempfile.TemporaryDirectory(prefix="warehouse-drift-") as temp_dir:
        regenerated_file = Path(temp_dir) / "warehouse.regenerated.dbml"
        schema_scope = os.getenv(
            "WAREHOUSE_SCHEMA_DRIFT_SCHEMAS",
            "job_market_nl,job_market_nl_dbt,snapshots,public",
        )
        command = [
            sys.executable,
            str(introspection_script),
            "--output-file",
            str(regenerated_file),
            "--stable",
            "--schemas",
            schema_scope,
        ]
        proc = subprocess.run(
            command,
            cwd=str(root),
            capture_output=True,
            text=True,
        )

        if proc.returncode != 0:
            diagnostic = (proc.stderr or proc.stdout or "").strip()
            message = "unable to regenerate warehouse schema via introspection"
            if diagnostic:
                message = f"{message}: {diagnostic.splitlines()[-1]}"
            return ValidationError(source_file=warehouse_dbml, message=message), None

        committed_lines = normalize_dbml_lines(warehouse_dbml.read_text(encoding="utf-8"))
        regenerated_lines = normalize_dbml_lines(regenerated_file.read_text(encoding="utf-8"))

        if committed_lines == regenerated_lines:
            return None, None

        diff = "\n".join(
            difflib.unified_diff(
                committed_lines,
                regenerated_lines,
                fromfile="schema/warehouse.dbml (committed)",
                tofile="schema/warehouse.dbml (regenerated)",
                lineterm="",
            )
        )
        return (
            ValidationError(
                source_file=warehouse_dbml,
                message="warehouse schema drift detected; regenerate and commit `schema/warehouse.dbml`",
            ),
            diff,
        )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Open Data Platform DBML schema files")
    parser.add_argument(
        "paths",
        nargs="*",
        help=(
            "DBML files or glob patterns. "
            "Defaults to schema/domains/*.dbml."
        ),
    )
    parser.add_argument(
        "--include-warehouse-structure",
        action="store_true",
        help=(
            "Also include schema/warehouse.dbml in structural DBML checks. "
            "By default, warehouse schema is only validated via --check-warehouse-drift."
        ),
    )
    parser.add_argument(
        "--strict-left-refs",
        action="store_true",
        help="Also validate `<` Ref targets (useful when lineage refs are fully modeled).",
    )
    parser.add_argument(
        "--check-warehouse-drift",
        action="store_true",
        help=(
            "Regenerate schema/warehouse.dbml from the live warehouse and fail "
            "if it differs from the committed file."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    root = project_root()
    dbml_files = expand_input_paths(
        args.paths,
        root,
        include_warehouse_structure=args.include_warehouse_structure,
    )

    if not dbml_files:
        print("No DBML files found to validate.")
        return 1

    errors, file_count, table_count = run_validation(dbml_files, strict_left_refs=args.strict_left_refs)
    drift_diff: Optional[str] = None

    if args.check_warehouse_drift:
        drift_error, drift_diff = run_warehouse_drift_check(root)
        if drift_error is not None:
            errors.append(drift_error)

    if errors:
        print(f"DBML validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error.format(root)}")
        if drift_diff:
            diff_lines = drift_diff.splitlines()
            print("\nWarehouse drift diff (committed vs regenerated):")
            print("\n".join(diff_lines[:120]))
            if len(diff_lines) > 120:
                print("... (diff truncated)")
        return 1

    print(f"DBML validation passed ({file_count} file(s), {table_count} table(s)).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
