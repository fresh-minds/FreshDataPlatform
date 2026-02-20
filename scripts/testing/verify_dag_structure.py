import sys
from unittest.mock import MagicMock

# Mock airflow modules
sys.modules["airflow"] = MagicMock()
sys.modules["airflow.operators"] = MagicMock()
sys.modules["airflow.operators.bash"] = MagicMock()

# Mock DAG class
class MockDAG:
    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        print(f"Initializing DAG: {dag_id}")
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass

sys.modules["airflow"].DAG = MockDAG

# Mock BashOperator
class MockBashOperator:
    def __init__(self, task_id, bash_command, **kwargs):
        self.task_id = task_id
        self.bash_command = bash_command
        print(f"  - Task created: {task_id}")
    
    def __rshift__(self, other):
        print(f"    Dependency: {self.task_id} >> {other.task_id if isinstance(other, MockBashOperator) else 'List'}")
        return other
    
    def __lshift__(self, other):
        return self

sys.modules["airflow.operators.bash"].BashOperator = MockBashOperator

# Mock PythonOperator
class MockPythonOperator:
    def __init__(self, task_id, python_callable, **kwargs):
        self.task_id = task_id
        print(f"  - PythonTask created: {task_id}")
    
    def __rshift__(self, other):
        print(f"    Dependency: {self.task_id} >> {other.task_id if hasattr(other, 'task_id') else 'Group/List'}")
        return other

sys.modules["airflow.operators.python"] = MagicMock()
sys.modules["airflow.operators.python"].PythonOperator = MockPythonOperator

# Mock TaskGroup
class MockTaskGroup:
    def __init__(self, group_id, **kwargs):
        self.group_id = group_id
        print(f"  [Group] Entering: {group_id}")
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): 
        print(f"  [Group] Exiting: {self.group_id}")
    def __rshift__(self, other):
        print(f"    Dependency: Group({self.group_id}) >> ...")
        return other

sys.modules["airflow.utils.task_group"] = MagicMock()
sys.modules["airflow.utils.task_group"].TaskGroup = MockTaskGroup

# Import the DAG to trigger execution
print("=== Verifying DAG Structure ===")
import os
sys.path.append(os.getcwd())
try:
    from dags import job_market_nl_dag
    print("\n=== DAG Structure Verified Successfully ===")
except Exception as e:
    print(f"\nERROR: DAG Verification Failed: {e}")
    sys.exit(1)
