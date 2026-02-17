"""
Runtime utilities for Spark session and Fabric context management.

Provides unified interface for getting Spark sessions and Fabric context
that works both locally and in Microsoft Fabric.
"""

import os
import sys
from typing import Any, Tuple

from pyspark.sql import SparkSession


def is_running_in_fabric() -> bool:
    """
    Detect if code is running inside Microsoft Fabric.

    Returns:
        True if running in Fabric, False if running locally.
    """
    # Check environment variable first (allows override)
    if os.getenv("IS_LOCAL", "").lower() == "true":
        return False

    # Check for Fabric-specific environment indicators
    fabric_indicators = [
        "FABRIC_WORKSPACE_ID",
        "SYNAPSE_POOL_NAME",
        "MSSPARKUTILS_PACKAGE",
    ]

    for indicator in fabric_indicators:
        if os.getenv(indicator):
            return True

    # Try to import sempy.fabric - only available in Fabric
    try:
        import sempy.fabric  # noqa: F401

        return True
    except ImportError:
        return False


def get_spark_session(app_name: str = "OpenDataPlatform") -> SparkSession:
    """
    Get or create a Spark session configured for the current environment.

    In Microsoft Fabric: Returns the pre-configured Spark session.
    Locally: Creates a new session with Delta Lake support.

    Args:
        app_name: Application name for the Spark session (local only)

    Returns:
        SparkSession configured for the environment
    """
    if is_running_in_fabric():
        # In Fabric, spark is pre-initialized
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    # Local configuration
    from dotenv import load_dotenv

    load_dotenv()

    # Ensure worker Python version matches driver
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Add hadoop-aws for MinIO support (Reverted to 3.5.3 compatible versions)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3,io.acryl:datahub-spark-lineage_2.12:0.12.0")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.master", "local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "false")
    )

    # Configure MinIO if enabled
    if os.getenv("USE_MINIO", "false").lower() == "true":
        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        print(f"[Spark] Configuring MinIO S3A endpoint: {minio_endpoint}")
        
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Fix NumberFormatException
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
            # Speed up S3 operations
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )

    # Configure DataHub if enabled
    if os.getenv("USE_DATAHUB", "false").lower() == "true":
        datahub_gms = os.getenv("DATAHUB_REST_URL", "http://localhost:8080")
        print(f"[Spark] Configuring DataHub lineage integration: {datahub_gms}")
        
        # DataHub Spark Lineage Config
        builder = (
            builder
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
            .config("spark.datahub.rest.server", datahub_gms)
        )

    return builder.getOrCreate()


def get_fabric_context() -> Tuple[Any, Any]:
    """
    Get fabric and notebookutils context for the current environment.

    Returns:
        Tuple of (notebookutils, fabric) objects - real or mocked.
    """
    if is_running_in_fabric():
        import notebookutils
        import sempy.fabric as fabric

        return notebookutils, fabric
    else:
        from shared.fabric.mocks import fabric, notebookutils

        return notebookutils, fabric


def get_fabric_client():
    """
    Get a Fabric REST client for the current environment.

    Returns:
        FabricRestClient (real or mocked)
    """
    _, fabric = get_fabric_context()
    return fabric.FabricRestClient()


def get_workspace_id() -> str:
    """
    Get the current workspace ID.

    Returns:
        Workspace ID string
    """
    _, fabric = get_fabric_context()
    return fabric.get_workspace_id()
