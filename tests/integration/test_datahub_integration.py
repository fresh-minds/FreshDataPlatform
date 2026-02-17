import os
import requests
import pytest
from scripts.datahub_client import DataHubService

DATAHUB_HOST = os.getenv("DATAHUB_GMS_HOST", "localhost")
# docker-compose exposes GMS on 8081 externally (maps container 8080 -> host 8081).
DATAHUB_PORT = os.getenv("DATAHUB_GMS_PORT", "8081")
DATAHUB_URL = f"http://{DATAHUB_HOST}:{DATAHUB_PORT}"

@pytest.fixture
def datahub_service():
    """Fixture to provide a DataHubService instance."""
    return DataHubService(host=DATAHUB_HOST, port=DATAHUB_PORT)

def test_datahub_health_check():
    """Verify that DataHub GMS is reachable."""
    try:
        response = requests.get(f"{DATAHUB_URL}/health")
        assert response.status_code == 200, f"DataHub GMS returned {response.status_code}"
    except Exception as e:
        pytest.fail(f"Failed to connect to DataHub GMS: {e}")

def test_datahub_emitter_connection(datahub_service):
    """Verify that the DataHub emitter can connect (test_connection)."""
    # This might fail if DataHub is not running or takes time to start
    # In CI, we usually wait for services to be ready
    try:
        datahub_service.emitter.test_connection()
    except Exception as e:
        pytest.skip(f"DataHub GMS not ready for emitter: {e}")

def test_datahub_emit_metadata(datahub_service):
    """Verify metadata emission (dry-run if possible, or actual emit)."""
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:postgres,test_table,PROD)"
    properties = {"test_key": "test_value"}
    
    try:
        datahub_service.emit_dataset_properties(dataset_urn, properties, description="Test description")
    except Exception as e:
        pytest.fail(f"Failed to emit metadata to DataHub: {e}")
