"""
Lakehouse path resolution for local and Fabric environments.

Provides a unified interface for constructing table paths that work
seamlessly in both local development and Microsoft Fabric.
"""

import os
from enum import Enum
from typing import Optional

from shared.config.settings import get_settings


class LakehouseLayer(str, Enum):
    """Data lakehouse layers following medallion architecture."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class LakehouseNames:
    """Standard lakehouse names by domain and layer."""

    FINANCE_BRONZE = "DE_LH_Finance_Bronze"
    FINANCE_SILVER = "DE_LH_Finance_Silver"
    FINANCE_GOLD = "DE_LH_Finance_Gold"


def get_lakehouse_table_path(
    table_name: str,
    layer: LakehouseLayer = LakehouseLayer.BRONZE,
    domain: str = "default",
    workspace_id: Optional[str] = None,
    lakehouse_id: Optional[str] = None,
    is_local: Optional[bool] = None,
) -> str:
    """
    Get the full path to a lakehouse table.

    This function abstracts away the difference between local file paths
    and Fabric ABFSS paths, returning the correct format based on environment.

    Args:
        table_name: Name of the table (e.g., "verkooporders")
        layer: Medallion layer (bronze, silver, gold)
        domain: Data domain (e.g., finance, sales, hr)
        workspace_id: Fabric workspace ID (optional, uses settings if not provided)
        lakehouse_id: Fabric lakehouse ID (optional, uses settings if not provided)
        is_local: Override environment detection (optional)

    Returns:
        Full path to the table (local path or ABFSS URI)

    Examples:
        Local: ./data/sales/bronze/orders
        Fabric: abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/dbo/verkooporders
    """
    settings = get_settings()

    if is_local is None:
        is_local = settings.is_local

    if is_local:
        # Check for MinIO usage
        use_minio = os.getenv("USE_MINIO", "false").lower()
        if use_minio == "true":
            # Map layer to bucket name
            bucket = layer.value
            path = f"s3a://{bucket}/{domain}/{table_name}"
            print(f"[Paths] using MinIO path: {path}")
            return path
        
        print(f"[Paths] using local path (USE_MINIO={use_minio})")
        # Local path: ./data/{domain}/{layer}/{table_name}
        base_path = os.getenv("LOCAL_LAKEHOUSE_PATH", settings.local.lakehouse_path)
        return os.path.join(base_path, domain, layer.value, table_name)

    # Fabric ABFSS path
    if workspace_id is None:
        workspace_id = settings.fabric.workspace_id

    if lakehouse_id is None:
        # Map layer to lakehouse ID from settings
        layer_to_id = {
            LakehouseLayer.BRONZE: settings.fabric.lakehouse_bronze_id,
            LakehouseLayer.SILVER: settings.fabric.lakehouse_silver_id,
            LakehouseLayer.GOLD: settings.fabric.lakehouse_gold_id,
        }
        lakehouse_id = layer_to_id.get(layer, settings.fabric.lakehouse_bronze_id)

    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/dbo/{table_name}"


def get_lakehouse_id_from_name(
    lakehouse_name: str,
    workspace_items_df: "pd.DataFrame",
) -> Optional[str]:
    """
    Look up lakehouse ID from a DataFrame of workspace items.

    Args:
        lakehouse_name: Display name of the lakehouse
        workspace_items_df: DataFrame from Fabric API with "type", "displayName", "id" columns

    Returns:
        Lakehouse ID if found, None otherwise
    """
    result = workspace_items_df.loc[
        (workspace_items_df["type"] == "Lakehouse") & (workspace_items_df["displayName"] == lakehouse_name),
        "id",
    ]
    return result.iloc[0] if not result.empty else None


def ensure_local_path_exists(path: str) -> str:
    """
    Ensure local directory exists for writing.

    Args:
        path: Path to the table directory

    Returns:
        The same path (for chaining)
    """
    settings = get_settings()
    if settings.is_local:
        os.makedirs(os.path.dirname(path), exist_ok=True)
    return path
