"""
Mock implementations of Microsoft Fabric components for local development.

These mocks simulate the behavior of notebookutils and fabric SDK when running
pipelines locally (outside of Microsoft Fabric).
"""

import os
from typing import Any


class CredentialsMock:
    """Mock for notebookutils.credentials that reads secrets from environment variables."""

    # Mapping of Azure Key Vault secret names to environment variable names
    SECRET_ENV_MAP = {
        # Add custom mappings here if your Key Vault uses non-standard names
    }

    def getSecret(self, vault_url: str, secret_name: str) -> str:
        """
        Get a secret value from environment variables.

        Args:
            vault_url: Azure Key Vault URL (ignored in local mode)
            secret_name: Name of the secret in Key Vault

        Returns:
            Secret value from corresponding environment variable
        """
        # Normalize secret name for lookup
        secret_key = secret_name.lower().replace("-", "").replace("_", "")

        # Check direct mapping first
        for kv_name, env_var in self.SECRET_ENV_MAP.items():
            if kv_name.lower().replace("-", "").replace("_", "") == secret_key:
                value = os.getenv(env_var)
                if value:
                    return value

        # Generic fallback
        return os.getenv(secret_name.upper().replace("-", "_"), f"mock_secret_for_{secret_name}")


class NotebookUtilsMock:
    """Mock for notebookutils module."""

    def __init__(self):
        self.credentials = CredentialsMock()

    def exit(self, value: str = "") -> None:
        """Mock notebook exit."""
        print(f"[NotebookUtils] Notebook exiting with: {value}")


class FabricRestClientMock:
    """Mock for Fabric REST API client."""

    def get(self, path: str) -> "MockResponse":
        """
        Mock GET request to Fabric API.

        Returns mock data for workspace items including lakehouses.
        """
        # Return mock response for workspace items endpoint
        if "/items" in path:
            return MockResponse(
                {
                    "value": [
                        {
                            "type": "Lakehouse",
                            "displayName": "DE_LH_Default_Bronze",
                            "id": os.getenv("LAKEHOUSE_BRONZE_ID", "local_bronze_lakehouse"),
                        },
                        {
                            "type": "Lakehouse",
                            "displayName": "DE_LH_Default_Silver",
                            "id": os.getenv("LAKEHOUSE_SILVER_ID", "local_silver_lakehouse"),
                        },
                        {
                            "type": "Lakehouse",
                            "displayName": "DE_LH_Default_Gold",
                            "id": os.getenv("LAKEHOUSE_GOLD_ID", "local_gold_lakehouse"),
                        },
                    ]
                }
            )
        return MockResponse({})

    def post(self, path: str, json: dict = None) -> "MockResponse":
        """Mock POST request."""
        return MockResponse({"status": "ok"})


class MockResponse:
    """Mock HTTP response object."""

    def __init__(self, data: dict, status_code: int = 200):
        self._data = data
        self._status_code = status_code

    def json(self) -> dict:
        return self._data

    @property
    def status_code(self) -> int:
        return self._status_code


class FabricMock:
    """Mock for sempy.fabric module."""

    def __init__(self):
        self._client = FabricRestClientMock()

    def FabricRestClient(self) -> FabricRestClientMock:
        """Return mock REST client."""
        return self._client

    def get_workspace_id(self) -> str:
        """Return mock workspace ID from environment."""
        return os.getenv("WORKSPACE_ID", "local_workspace")


# Global mock instances for easy import
notebookutils = NotebookUtilsMock()
fabric = FabricMock()
