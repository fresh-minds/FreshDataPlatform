import importlib
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock


def _install_airflow_mocks() -> None:
    airflow_mod = MagicMock()
    sys.modules["airflow"] = airflow_mod

    class _MockDependencyNode:
        def __init__(self, task_id: str | None = None, **kwargs):
            self.task_id = task_id or kwargs.get("group_id") or "anonymous"
            print(f"  - Node created: {self.task_id}")

        def __rshift__(self, other):
            return other

        def __lshift__(self, other):
            return other

    class MockDAG:
        def __init__(self, dag_id: str, **kwargs):
            self.dag_id = dag_id
            print(f"Initializing DAG: {dag_id}")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

    class MockTaskGroup(_MockDependencyNode):
        def __init__(self, group_id: str, **kwargs):
            super().__init__(task_id=group_id, **kwargs)
            self.group_id = group_id
            print(f"  [Group] Entering: {group_id}")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            print(f"  [Group] Exiting: {self.group_id}")
            return False

    airflow_mod.DAG = MockDAG

    sys.modules["airflow.operators"] = MagicMock()

    bash_module = ModuleType("airflow.operators.bash")
    bash_module.BashOperator = _MockDependencyNode
    sys.modules["airflow.operators.bash"] = bash_module

    python_module = ModuleType("airflow.operators.python")
    python_module.PythonOperator = _MockDependencyNode
    sys.modules["airflow.operators.python"] = python_module

    empty_module = ModuleType("airflow.operators.empty")
    empty_module.EmptyOperator = _MockDependencyNode
    sys.modules["airflow.operators.empty"] = empty_module

    task_group_module = ModuleType("airflow.utils.task_group")
    task_group_module.TaskGroup = MockTaskGroup
    sys.modules["airflow.utils.task_group"] = task_group_module


def _dag_modules(dags_dir: Path) -> list[str]:
    expected_files = [
        "job_market_nl_dag.py",
        "source_sp1_vacatures_ingestion.py",
    ]
    modules: list[str] = []
    for file_name in expected_files:
        if (dags_dir / file_name).exists():
            modules.append(f"dags.{Path(file_name).stem}")
    return modules


def main() -> int:
    print("=== Verifying DAG Structure ===")
    _install_airflow_mocks()

    repo_root = Path(os.getcwd())
    dags_dir = repo_root / "dags"
    sys.path.append(str(repo_root))

    failures: list[tuple[str, str]] = []
    for module_name in _dag_modules(dags_dir):
        try:
            importlib.import_module(module_name)
            print(f"✔ Imported {module_name}")
        except Exception as exc:  # noqa: BLE001
            failures.append((module_name, str(exc)))
            print(f"✖ Failed {module_name}: {exc}")

    if failures:
        print("\nERROR: DAG Verification Failed")
        for module_name, error in failures:
            print(f"- {module_name}: {error}")
        return 1

    print("\n=== DAG Structure Verified Successfully ===")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
