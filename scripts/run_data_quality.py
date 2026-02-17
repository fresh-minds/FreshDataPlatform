#!/usr/bin/env python3
"""
Run centralized data quality checks from a single YAML rules file.

Usage examples:
  python scripts/run_data_quality.py --list-datasets
  python scripts/run_data_quality.py --dataset job_market_nl.it_market_snapshot
  python scripts/run_data_quality.py --all
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from contextlib import nullcontext

import yaml

try:
    from pyspark.sql import DataFrame, SparkSession, functions as F
except ModuleNotFoundError:  # pragma: no cover - allows list/config checks without pyspark
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

# Ensure project root is on sys.path when script is executed directly.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RULES_FILE_DEFAULT = PROJECT_ROOT / "schema" / "data_quality_rules.yaml"
ALLOWED_SEVERITIES = {"low", "medium", "high"}
ALLOWED_MISSING_BEHAVIOR = {"fail", "warn", "skip"}


@dataclass
class CheckResult:
    dataset: str
    name: str
    check_type: str
    severity: str
    passed: bool
    detail: str


class DataQualityError(RuntimeError):
    """Raised for invalid data quality configuration."""


class DataQualityRunner:
    def __init__(self, spark: SparkSession, rules: dict[str, Any], rules_file: Path) -> None:
        self.spark = spark
        self.rules = rules
        self.rules_file = rules_file
        self.datasets: dict[str, dict[str, Any]] = rules.get("datasets", {})
        self.defaults: dict[str, Any] = rules.get("defaults", {})
        self._df_cache: dict[str, DataFrame] = {}
        self._count_cache: dict[str, int] = {}
        from shared.config.settings import get_settings

        self.settings = get_settings()

    def list_datasets(self) -> list[str]:
        return sorted(self.datasets.keys())

    def run_dataset(self, dataset_key: str, fail_on: set[str]) -> Tuple[list[CheckResult], bool]:
        if dataset_key not in self.datasets:
            raise DataQualityError(f"dataset `{dataset_key}` is not defined in {self.rules_file}")

        dataset_cfg = self.datasets[dataset_key]
        checks = dataset_cfg.get("checks", [])
        if not checks:
            raise DataQualityError(f"dataset `{dataset_key}` has no checks configured")

        try:
            df = self._load_dataset(dataset_key)
        except Exception as exc:  # noqa: BLE001
            missing_behavior = str(dataset_cfg.get("missing_behavior", "fail")).lower()
            if missing_behavior not in ALLOWED_MISSING_BEHAVIOR:
                raise DataQualityError(
                    f"dataset `{dataset_key}` has invalid missing_behavior `{missing_behavior}`"
                ) from exc

            if missing_behavior == "skip":
                result = CheckResult(
                    dataset=dataset_key,
                    name="dataset_presence",
                    check_type="dataset_load",
                    severity="low",
                    passed=True,
                    detail=f"skipped (dataset unavailable): {exc}",
                )
                return [result], False

            severity = "medium" if missing_behavior == "warn" else "high"
            result = CheckResult(
                dataset=dataset_key,
                name="dataset_presence",
                check_type="dataset_load",
                severity=severity,
                passed=False,
                detail=f"unable to load dataset: {exc}",
            )
            should_fail = severity in fail_on
            return [result], should_fail

        row_count = self._row_count(dataset_key, df)
        results: list[CheckResult] = []
        has_failures = False

        for raw_check in checks:
            result = self._run_check(dataset_key, df, row_count, raw_check)
            results.append(result)
            if not result.passed and result.severity in fail_on:
                has_failures = True

        return results, has_failures

    def _run_check(
        self,
        dataset_key: str,
        df: DataFrame,
        row_count: int,
        check: dict[str, Any],
    ) -> CheckResult:
        check_type = str(check.get("type", "")).strip()
        if not check_type:
            raise DataQualityError(f"dataset `{dataset_key}` has check without `type`")

        name = str(check.get("name", check_type))
        severity = str(check.get("severity", self.defaults.get("default_severity", "high"))).lower()
        if severity not in ALLOWED_SEVERITIES:
            raise DataQualityError(
                f"dataset `{dataset_key}` check `{name}` has invalid severity `{severity}`"
            )

        handlers = {
            "columns_exist": self._check_columns_exist,
            "row_count_between": self._check_row_count_between,
            "non_null": self._check_non_null,
            "unique": self._check_unique,
            "accepted_values": self._check_accepted_values,
            "value_range": self._check_value_range,
            "regex": self._check_regex,
            "freshness_hours": self._check_freshness_hours,
            "expression": self._check_expression,
            "foreign_key_exists": self._check_foreign_key_exists,
        }

        handler = handlers.get(check_type)
        if handler is None:
            raise DataQualityError(
                f"dataset `{dataset_key}` check `{name}` has unsupported type `{check_type}`"
            )

        passed, detail = handler(dataset_key, df, row_count, check)
        return CheckResult(
            dataset=dataset_key,
            name=name,
            check_type=check_type,
            severity=severity,
            passed=passed,
            detail=detail,
        )

    def _dataset_config(self, dataset_key: str) -> dict[str, Any]:
        return self.datasets[dataset_key]

    def _resolve_path(self, dataset_key: str, dataset_cfg: dict[str, Any]) -> str:
        from shared.config.paths import LakehouseLayer, get_lakehouse_table_path

        if "path" in dataset_cfg:
            return str(dataset_cfg["path"])

        domain = dataset_cfg.get("domain")
        table = dataset_cfg.get("table")
        layer = dataset_cfg.get("layer")
        if not all([domain, table, layer]):
            raise DataQualityError(
                f"dataset `{dataset_key}` must define either `path` or `domain`+`layer`+`table`"
            )

        try:
            layer_enum = LakehouseLayer(str(layer).lower())
        except ValueError as exc:
            raise DataQualityError(
                f"dataset `{dataset_key}` has invalid layer `{layer}`"
            ) from exc

        return get_lakehouse_table_path(
            table_name=str(table),
            layer=layer_enum,
            domain=str(domain),
        )

    def _load_dataset(self, dataset_key: str) -> DataFrame:
        from shared.utils.export_helper import read_lakehouse_table

        if dataset_key in self._df_cache:
            return self._df_cache[dataset_key]

        cfg = self._dataset_config(dataset_key)
        path = self._resolve_path(dataset_key, cfg)
        if self.settings.is_local and not path.startswith("s3a://") and not os.path.exists(path):
            raise FileNotFoundError(path)

        df = read_lakehouse_table(self.spark, path)
        self._df_cache[dataset_key] = df
        return df

    def _row_count(self, dataset_key: str, df: DataFrame) -> int:
        if dataset_key in self._count_cache:
            return self._count_cache[dataset_key]
        value = df.count()
        self._count_cache[dataset_key] = value
        return value

    def _check_columns_exist(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key, row_count
        expected = check.get("columns", [])
        if not isinstance(expected, list) or not expected:
            raise DataQualityError("columns_exist requires non-empty `columns`")

        missing = [c for c in expected if c not in df.columns]
        if missing:
            return False, f"missing columns: {missing}"
        return True, f"all required columns exist ({len(expected)})"

    def _check_row_count_between(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key, df
        min_rows = check.get("min")
        max_rows = check.get("max")
        if min_rows is None and max_rows is None:
            raise DataQualityError("row_count_between requires `min` and/or `max`")

        if min_rows is not None and row_count < int(min_rows):
            return False, f"row_count={row_count} < min={min_rows}"
        if max_rows is not None and row_count > int(max_rows):
            return False, f"row_count={row_count} > max={max_rows}"
        return True, f"row_count={row_count} within configured range"

    def _check_non_null(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        columns = check.get("columns", [])
        max_null_pct = float(check.get("max_null_pct", 0.0))
        allow_empty = bool(check.get("allow_empty", False))
        if not isinstance(columns, list) or not columns:
            raise DataQualityError("non_null requires non-empty `columns`")

        missing = [c for c in columns if c not in df.columns]
        if missing:
            return False, f"columns not found: {missing}"

        if row_count == 0:
            if allow_empty:
                return True, "dataset empty and allow_empty=true"
            return False, "dataset empty"

        aggregations = [
            F.sum(F.when(F.col(column).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(column)
            for column in columns
        ]
        null_counts = df.agg(*aggregations).collect()[0].asDict()

        failures = []
        for column in columns:
            null_count = int(null_counts.get(column, 0))
            null_pct = (null_count / row_count) * 100
            if null_pct > max_null_pct:
                failures.append(f"{column}={null_pct:.2f}% > {max_null_pct:.2f}%")

        if failures:
            return False, "; ".join(failures)
        return True, f"null percentages within threshold ({max_null_pct:.2f}%)"

    def _check_unique(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        columns = check.get("columns", [])
        max_duplicate_pct = float(check.get("max_duplicate_pct", 0.0))
        if not isinstance(columns, list) or not columns:
            raise DataQualityError("unique requires non-empty `columns`")

        missing = [c for c in columns if c not in df.columns]
        if missing:
            return False, f"columns not found: {missing}"

        if row_count == 0:
            return True, "dataset empty"

        distinct_count = df.select(*columns).dropDuplicates().count()
        duplicate_count = row_count - distinct_count
        duplicate_pct = (duplicate_count / row_count) * 100
        if duplicate_pct > max_duplicate_pct:
            return (
                False,
                f"duplicate_pct={duplicate_pct:.2f}% > {max_duplicate_pct:.2f}% "
                f"(duplicate_rows={duplicate_count})",
            )
        return True, f"duplicate_pct={duplicate_pct:.2f}% within threshold"

    def _check_accepted_values(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        column = check.get("column")
        allowed_values = check.get("allowed_values", [])
        max_invalid_pct = float(check.get("max_invalid_pct", 0.0))
        if not column or not isinstance(column, str):
            raise DataQualityError("accepted_values requires `column`")
        if not isinstance(allowed_values, list) or not allowed_values:
            raise DataQualityError("accepted_values requires non-empty `allowed_values`")
        if column not in df.columns:
            return False, f"column not found: {column}"

        if row_count == 0:
            return True, "dataset empty"

        invalid_count = (
            df.filter(F.col(column).isNotNull() & (~F.col(column).isin(*allowed_values))).count()
        )
        invalid_pct = (invalid_count / row_count) * 100
        if invalid_pct > max_invalid_pct:
            return (
                False,
                f"invalid_pct={invalid_pct:.2f}% > {max_invalid_pct:.2f}% "
                f"(invalid_count={invalid_count})",
            )
        return True, f"invalid_pct={invalid_pct:.2f}% within threshold"

    def _check_value_range(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        column = check.get("column")
        min_value = check.get("min")
        max_value = check.get("max")
        max_invalid_pct = float(check.get("max_invalid_pct", 0.0))
        ignore_nulls = bool(check.get("ignore_nulls", True))
        if not column or not isinstance(column, str):
            raise DataQualityError("value_range requires `column`")
        if min_value is None and max_value is None:
            raise DataQualityError("value_range requires `min` and/or `max`")
        if column not in df.columns:
            return False, f"column not found: {column}"
        if row_count == 0:
            return True, "dataset empty"

        value_col = F.col(column).cast("double")
        invalid_condition = F.lit(False)
        if min_value is not None:
            invalid_condition = invalid_condition | (value_col < float(min_value))
        if max_value is not None:
            invalid_condition = invalid_condition | (value_col > float(max_value))
        if not ignore_nulls:
            invalid_condition = invalid_condition | value_col.isNull()

        invalid_count = df.filter(invalid_condition).count()
        invalid_pct = (invalid_count / row_count) * 100
        if invalid_pct > max_invalid_pct:
            return (
                False,
                f"invalid_pct={invalid_pct:.2f}% > {max_invalid_pct:.2f}% "
                f"(invalid_count={invalid_count})",
            )
        return True, f"invalid_pct={invalid_pct:.2f}% within threshold"

    def _check_regex(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        column = check.get("column")
        pattern = check.get("pattern")
        max_invalid_pct = float(check.get("max_invalid_pct", 0.0))
        ignore_nulls = bool(check.get("ignore_nulls", True))
        if not column or not isinstance(column, str):
            raise DataQualityError("regex requires `column`")
        if not pattern or not isinstance(pattern, str):
            raise DataQualityError("regex requires `pattern`")
        if column not in df.columns:
            return False, f"column not found: {column}"
        if row_count == 0:
            return True, "dataset empty"

        condition = ~F.col(column).rlike(pattern)
        if ignore_nulls:
            condition = F.col(column).isNotNull() & condition
        invalid_count = df.filter(condition).count()
        invalid_pct = (invalid_count / row_count) * 100
        if invalid_pct > max_invalid_pct:
            return (
                False,
                f"invalid_pct={invalid_pct:.2f}% > {max_invalid_pct:.2f}% "
                f"(invalid_count={invalid_count})",
            )
        return True, f"invalid_pct={invalid_pct:.2f}% within threshold"

    def _check_freshness_hours(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key, row_count
        column = check.get("column")
        max_age_hours = float(check.get("max_age_hours", 24.0))
        if not column or not isinstance(column, str):
            raise DataQualityError("freshness_hours requires `column`")
        if column not in df.columns:
            return False, f"column not found: {column}"

        max_ts = (
            df.select(F.max(F.to_timestamp(F.col(column))).alias("max_ts")).collect()[0]["max_ts"]
        )
        if max_ts is None:
            return False, f"no parseable timestamp found in column `{column}`"

        now_utc = datetime.now(timezone.utc)
        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        age_hours = (now_utc - max_ts).total_seconds() / 3600
        if age_hours > max_age_hours:
            return False, f"age_hours={age_hours:.2f} > max_age_hours={max_age_hours:.2f}"
        return True, f"age_hours={age_hours:.2f} within threshold"

    def _check_expression(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = dataset_key
        expression = check.get("expression")
        max_invalid_pct = float(check.get("max_invalid_pct", 0.0))
        if not expression or not isinstance(expression, str):
            raise DataQualityError("expression requires SQL `expression`")
        if row_count == 0:
            return True, "dataset empty"

        expr_col = F.expr(expression)
        invalid_condition = F.coalesce(~expr_col, F.lit(True))
        invalid_count = df.filter(invalid_condition).count()
        invalid_pct = (invalid_count / row_count) * 100
        if invalid_pct > max_invalid_pct:
            return (
                False,
                f"invalid_pct={invalid_pct:.2f}% > {max_invalid_pct:.2f}% "
                f"(invalid_count={invalid_count})",
            )
        return True, f"invalid_pct={invalid_pct:.2f}% within threshold"

    def _check_foreign_key_exists(
        self, dataset_key: str, df: DataFrame, row_count: int, check: dict[str, Any]
    ) -> Tuple[bool, str]:
        _ = row_count
        columns = check.get("columns", [])
        reference_dataset = check.get("reference_dataset")
        reference_columns = check.get("reference_columns", [])
        max_orphan_pct = float(check.get("max_orphan_pct", 0.0))
        allow_nulls = bool(check.get("allow_nulls", True))

        if not isinstance(columns, list) or not columns:
            raise DataQualityError("foreign_key_exists requires non-empty `columns`")
        if not isinstance(reference_columns, list) or not reference_columns:
            raise DataQualityError("foreign_key_exists requires non-empty `reference_columns`")
        if len(columns) != len(reference_columns):
            raise DataQualityError("foreign_key_exists requires same number of columns and reference_columns")
        if not isinstance(reference_dataset, str) or not reference_dataset:
            raise DataQualityError("foreign_key_exists requires `reference_dataset`")

        missing = [c for c in columns if c not in df.columns]
        if missing:
            return False, f"columns not found: {missing}"

        ref_df = self._load_dataset(reference_dataset)
        ref_missing = [c for c in reference_columns if c not in ref_df.columns]
        if ref_missing:
            return False, f"reference columns not found in {reference_dataset}: {ref_missing}"

        left_df = df.select(*[F.col(c).alias(c) for c in columns])
        if allow_nulls:
            null_condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
            left_df = left_df.filter(~null_condition)

        left_count = left_df.count()
        if left_count == 0:
            return True, "no non-null key rows to validate"

        ref_selected = ref_df.select(*[F.col(c).alias(c) for c in reference_columns]).dropDuplicates()
        ref_renamed = ref_selected
        for source, target in zip(reference_columns, columns):
            ref_renamed = ref_renamed.withColumnRenamed(source, target)

        join_condition = reduce(
            lambda a, b: a & b,
            [F.col(f"l.{c}") == F.col(f"r.{c}") for c in columns],
        )
        orphans = (
            left_df.alias("l").join(ref_renamed.alias("r"), join_condition, "left_anti").count()
        )
        orphan_pct = (orphans / left_count) * 100
        if orphan_pct > max_orphan_pct:
            return False, f"orphan_pct={orphan_pct:.2f}% > {max_orphan_pct:.2f}% (orphan_rows={orphans})"
        return True, f"orphan_pct={orphan_pct:.2f}% within threshold"


def load_rules(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DataQualityError(f"rules file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if "datasets" not in payload or not isinstance(payload["datasets"], dict):
        raise DataQualityError("rules file must include `datasets` mapping")
    return payload


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run centralized data quality checks")
    parser.add_argument(
        "--rules-file",
        default=str(RULES_FILE_DEFAULT),
        help=f"Path to rules YAML (default: {RULES_FILE_DEFAULT})",
    )
    parser.add_argument("--dataset", help="Dataset key from rules file (e.g. job_market_nl.it_market_snapshot)")
    parser.add_argument("--all", action="store_true", help="Run checks for all datasets")
    parser.add_argument(
        "--fail-on",
        default="high",
        help="Comma-separated severities that should fail the command (default: high)",
    )
    parser.add_argument("--list-datasets", action="store_true", help="List configured dataset keys")
    return parser.parse_args(argv)


def print_results(results: Iterable[CheckResult]) -> None:
    for item in results:
        status = "PASS" if item.passed else "FAIL"
        print(
            f"[{status}] dataset={item.dataset} check={item.name} "
            f"type={item.check_type} severity={item.severity} detail={item.detail}"
        )


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    rules_file = Path(args.rules_file).resolve()
    rules = load_rules(rules_file)

    if args.dataset and args.all:
        raise DataQualityError("use either --dataset or --all, not both")

    if args.list_datasets:
        print("Configured datasets:")
        for key in sorted(rules.get("datasets", {}).keys()):
            print(f"  - {key}")
        return 0

    if args.all:
        target_datasets = sorted(rules.get("datasets", {}).keys())
    elif args.dataset:
        target_datasets = [args.dataset]
    else:
        raise DataQualityError("provide one of --dataset, --all, or --list-datasets")

    fail_on = {part.strip().lower() for part in args.fail_on.split(",") if part.strip()}
    invalid = fail_on - ALLOWED_SEVERITIES
    if invalid:
        raise DataQualityError(f"invalid severity in --fail-on: {sorted(invalid)}")

    if F is None:
        raise DataQualityError(
            "pyspark is not installed in this environment; "
            "install dependencies before running data quality checks"
        )

    from shared.fabric.runtime import get_spark_session
    from shared.observability import get_observability

    spark = get_spark_session("DataQualityChecks")
    runner = DataQualityRunner(spark, rules, rules_file)
    observability = get_observability("open-data-platform-data-quality")
    tracer = observability.tracer
    dq_metrics = observability.data_quality_metrics

    all_results: list[CheckResult] = []
    should_fail = False

    run_span = tracer.start_as_current_span(
        "data_quality.run",
        attributes={
            "dq.dataset_count": len(target_datasets),
            "dq.fail_on": ",".join(sorted(fail_on)),
        },
    ) if tracer else nullcontext()

    with run_span:
        for dataset_key in target_datasets:
            dataset_start = time.perf_counter()
            span_ctx = tracer.start_as_current_span(
                "data_quality.dataset",
                attributes={"dq.dataset": dataset_key},
            ) if tracer else nullcontext()
            with span_ctx:
                results, has_failures = runner.run_dataset(dataset_key, fail_on=fail_on)
            duration = time.perf_counter() - dataset_start
            if dq_metrics:
                dq_metrics.record_dataset_run(dataset_key, duration)
                for result in results:
                    dq_metrics.record_check(
                        dataset=result.dataset,
                        check=result.name,
                        severity=result.severity,
                        passed=result.passed,
                    )
            all_results.extend(results)
            should_fail = should_fail or has_failures

    print_results(all_results)
    total = len(all_results)
    failed = len([r for r in all_results if not r.passed])
    print(f"\nData quality checks complete: {total - failed}/{total} passed, {failed} failed.")
    return 1 if should_fail else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except DataQualityError as exc:
        print(f"Configuration error: {exc}")
        raise SystemExit(2)
