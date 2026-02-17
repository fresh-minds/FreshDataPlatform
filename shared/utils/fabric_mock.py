from typing import Any, Optional

class FabricMock:
    """Mock for Microsoft Fabric SDK and notebookutils."""
    
    class Credentials:
        def getSecret(self, vault_url: str, key: str) -> str:
            return "mock_secret_value"
            
    def __init__(self):
        self.credentials = self.Credentials()
        
    def get_workspace_id(self) -> str:
        return "mock_workspace_id"
        
    def get_lakehouse_id(self) -> str:
        return "mock_lakehouse_id"
        
    # Mock MSSparkUtils.credentials.getSecret
    @property
    def notebookutils(self):
        return self

    def FabricRestClient(self):
        return self
        
    def get(self, url: str):
        # Mock response for REST calls if any
        class MockResponse:
            def json(self):
                return {"value": []}
        return MockResponse()
