from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg2
import requests
import yaml
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from tests.helpers.qa_config import AirflowConfig, MetadataConfig, WarehouseConfig


class WarehouseQueryError(RuntimeError):
    """Raised for warehouse query or connection failures."""


def split_dataset_name(dataset: str) -> tuple[str, str]:
    parts = dataset.split(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Expected dataset name in 'schema.table' format, got: {dataset}")
    return parts[0], parts[1]


def normalize_dataset_name(dataset: str) -> str:
    return dataset.strip().lower().replace('"', "")


class SQLWarehouseConnector:
    def __init__(self, cfg: WarehouseConfig) -> None:
        self.cfg = cfg

    def _connect(self):
        return psycopg2.connect(
            host=self.cfg.host,
            port=self.cfg.port,
            dbname=self.cfg.dbname,
            user=self.cfg.user,
            password=self.cfg.password,
        )

    def ping(self) -> None:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise WarehouseQueryError(f"Unable to connect to warehouse ({self.cfg.dsn}): {exc}") from exc

    def query_rows(self, query: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    return list(cur.fetchall())
        except Exception as exc:  # noqa: BLE001
            raise WarehouseQueryError(f"Query failed: {exc}\nSQL:\n{query}") from exc

    def query_scalar(self, query: str, params: Sequence[Any] | None = None) -> Any:
        rows = self.query_rows(query, params)
        if not rows:
            return None
        first = rows[0]
        if not first:
            return None
        return list(first.values())[0]

    def execute(self, query: str, params: Sequence[Any] | None = None) -> None:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise WarehouseQueryError(f"Statement failed: {exc}\nSQL:\n{query}") from exc

    def table_exists(self, dataset: str) -> bool:
        schema_name, table_name = split_dataset_name(dataset)
        query = """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        """
        return bool(self.query_rows(query, (schema_name, table_name)))

    def view_exists(self, dataset: str) -> bool:
        schema_name, table_name = split_dataset_name(dataset)
        query = """
            SELECT 1
            FROM information_schema.views
            WHERE table_schema = %s
              AND table_name = %s
        """
        return bool(self.query_rows(query, (schema_name, table_name)))

    def row_count(self, dataset: str) -> int:
        schema_name, table_name = split_dataset_name(dataset)
        query = sql.SQL("SELECT COUNT(*)::bigint AS c FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            return int(row[0] if row else 0)
        except Exception as exc:  # noqa: BLE001
            raise WarehouseQueryError(f"Unable to count rows for {dataset}: {exc}") from exc

    def get_columns(self, dataset: str) -> list[dict[str, Any]]:
        schema_name, table_name = split_dataset_name(dataset)
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
        """
        return self.query_rows(query, (schema_name, table_name))

    def column_names(self, dataset: str) -> list[str]:
        return [str(row["column_name"]) for row in self.get_columns(dataset)]

    def has_select_privilege(self, role_name: str, dataset: str) -> bool:
        schema_name, table_name = split_dataset_name(dataset)
        fully_qualified = f"{schema_name}.{table_name}"
        query = "SELECT has_table_privilege(%s, %s, 'SELECT') AS allowed"
        value = self.query_scalar(query, (role_name, fully_qualified))
        return bool(value)

    def select_checksum(self, dataset: str, columns: Iterable[str]) -> str:
        schema_name, table_name = split_dataset_name(dataset)
        cols = [c.strip() for c in columns if c and c.strip()]
        if not cols:
            raise ValueError("At least one checksum column is required")

        escaped_cols = [sql.Identifier(col) for col in cols]
        query = sql.SQL(
            """
            SELECT md5(string_agg(row_hash, '|' ORDER BY row_hash)) AS checksum
            FROM (
              SELECT md5(concat_ws('||', {})) AS row_hash
              FROM {}.{}
            ) t
            """
        ).format(
            sql.SQL(", ").join(
                sql.SQL("COALESCE({}::text, '')").format(identifier) for identifier in escaped_cols
            ),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            checksum = row[0] if row else None
            return checksum or ""
        except Exception as exc:  # noqa: BLE001
            raise WarehouseQueryError(f"Unable to compute checksum for {dataset}: {exc}") from exc


@dataclass(frozen=True)
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_s: float


class DbtCommandRunner:
    def __init__(self, repo_root: Path) -> None:
        self.repo_root = repo_root
        self.dbt_bin = repo_root / ".venv" / "bin" / "dbt"

    def is_available(self) -> bool:
        if self.dbt_bin.exists():
            return True
        return bool(shutil.which("dbt"))

    def _resolve_dbt_bin(self) -> str:
        if self.dbt_bin.exists():
            return str(self.dbt_bin)
        fallback = shutil.which("dbt")
        if fallback:
            return fallback
        raise FileNotFoundError("dbt binary not found (.venv/bin/dbt or PATH)")

    def run(self, args: Sequence[str], timeout_s: int = 1800) -> CommandResult:
        dbt_bin = self._resolve_dbt_bin()
        command = [dbt_bin, *args]
        started = time.perf_counter()
        completed = subprocess.run(
            command,
            cwd=str(self.repo_root),
            text=True,
            capture_output=True,
            timeout=timeout_s,
            env=os.environ.copy(),
            check=False,
        )
        duration_s = time.perf_counter() - started
        return CommandResult(
            command=command,
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_s=duration_s,
        )


class AirflowConnector:
    def __init__(self, cfg: AirflowConfig) -> None:
        self.cfg = cfg
        self.session = requests.Session()
        self.session.auth = (cfg.username, cfg.password)
        self.session.headers.update({"Content-Type": "application/json"})

    def health(self) -> bool:
        try:
            response = self.session.get(f"{self.cfg.base_url}/health", timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def trigger_dag_run(self, dag_run_id: str, conf: dict[str, Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"dag_run_id": dag_run_id}
        if conf:
            payload["conf"] = conf

        response = self.session.post(
            f"{self.cfg.base_url}/api/v1/dags/{self.cfg.dag_id}/dagRuns",
            data=json.dumps(payload),
            timeout=15,
        )
        if response.status_code not in {200, 201, 409}:
            raise RuntimeError(
                f"Failed to trigger DAG run: HTTP {response.status_code} {response.text}"
            )
        return response.json() if response.text else {"status": "unknown"}

    def get_dag_run_state(self, dag_run_id: str) -> str:
        response = self.session.get(
            f"{self.cfg.base_url}/api/v1/dags/{self.cfg.dag_id}/dagRuns/{dag_run_id}",
            timeout=15,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to fetch DAG run state: HTTP {response.status_code} {response.text}"
            )
        payload = response.json()
        return str(payload.get("state", "unknown"))

    def wait_for_completion(self, dag_run_id: str, timeout_s: int = 900, poll_s: int = 10) -> str:
        started = time.time()
        while (time.time() - started) <= timeout_s:
            state = self.get_dag_run_state(dag_run_id)
            if state in {"success", "failed"}:
                return state
            time.sleep(poll_s)
        raise TimeoutError(
            f"Timed out waiting for DAG run {dag_run_id} to finish after {timeout_s} seconds"
        )


class MetadataStoreConnector:
    def __init__(self, repo_root: Path, cfg: MetadataConfig) -> None:
        self.repo_root = repo_root
        self.cfg = cfg

    def datahub_healthy(self) -> bool:
        try:
            response = requests.get(f"{self.cfg.datahub_gms_url}/health", timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def load_metric_registry(self) -> dict[str, Any]:
        path = self.repo_root / "schema" / "metrics.yaml"
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError("schema/metrics.yaml must contain a top-level mapping")
        return payload

    def has_lineage_for_dataset(self, dataset: str) -> bool:
        target = normalize_dataset_name(dataset)
        registry = self.load_metric_registry()
        metrics = registry.get("metrics", [])
        if not isinstance(metrics, list):
            return False

        for metric in metrics:
            if not isinstance(metric, dict):
                continue
            source_dataset = normalize_dataset_name(str((metric.get("source") or {}).get("dataset", "")))
            if source_dataset != target:
                continue
            lineage = metric.get("lineage") or {}
            upstreams = lineage.get("upstream_datasets", [])
            if isinstance(upstreams, list) and len(upstreams) > 0:
                return True
        return False
