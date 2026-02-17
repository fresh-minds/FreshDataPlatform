"""
Application settings using Pydantic for type-safe configuration.

Loads configuration from environment variables with sensible defaults
for both local development and Microsoft Fabric execution.
"""

import os
from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Load .env file if it exists
load_dotenv()


class FabricSettings(BaseSettings):
    """Microsoft Fabric configuration."""

    model_config = SettingsConfigDict(extra="ignore")

    workspace_id: str = Field(default="local_workspace", alias="WORKSPACE_ID")
    lakehouse_bronze_id: str = Field(default="local_bronze", alias="LAKEHOUSE_BRONZE_ID")
    lakehouse_silver_id: str = Field(default="local_silver", alias="LAKEHOUSE_SILVER_ID")
    lakehouse_gold_id: str = Field(default="local_gold", alias="LAKEHOUSE_GOLD_ID")

    # Key Vault URL for secret retrieval in Fabric
    key_vault_url: str = Field(
        default="https://dev-kv-fabric-sales.vault.azure.net/",
        alias="KEY_VAULT_URL",
    )


class LocalSettings(BaseSettings):
    """Local development settings."""

    model_config = SettingsConfigDict(extra="ignore")

    is_local: bool = Field(default=False, alias="IS_LOCAL")
    lakehouse_path: str = Field(default="./data", alias="LOCAL_LAKEHOUSE_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")


class O365Settings(BaseSettings):
    """Office 365 API configuration."""

    model_config = SettingsConfigDict(env_prefix="O365_", extra="ignore")

    client_id: str = Field(default="")
    client_secret: Optional[str] = Field(default=None)
    tenant_id: str = Field(default="")
    # Authority URL for MSAL/Graph
    authority: str = Field(default="https://login.microsoftonline.com")
    scopes: str = Field(default="https://graph.microsoft.com/.default")


class Settings(BaseSettings):
    """Main settings container aggregating all configuration."""

    model_config = SettingsConfigDict(extra="ignore")

    o365: O365Settings = Field(default_factory=O365Settings)
    fabric: FabricSettings = Field(default_factory=FabricSettings)
    local: LocalSettings = Field(default_factory=LocalSettings)

    @property
    def is_local(self) -> bool:
        """Check if running in local mode."""
        return self.local.is_local or os.getenv("IS_LOCAL", "").lower() == "true"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Settings are loaded once and cached for the lifetime of the application.
    Call this function to get type-safe access to all configuration.

    Returns:
        Settings instance with all configuration loaded
    """
    return Settings()
