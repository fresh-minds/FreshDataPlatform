"""Notebook helpers for MinIO and Postgres access in local platform runs."""

from __future__ import annotations

import io
import os
from typing import Any, Dict, List

import boto3
import pandas as pd
from botocore.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _as_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def minio_client() -> Any:
    """Create a MinIO-compatible S3 client from environment variables."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "admin")
    verify_tls = _as_bool("MINIO_SECURE", endpoint.startswith("https://"))

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        verify=verify_tls,
    )


def list_minio_objects(bucket: str, prefix: str = "", max_keys: int = 200) -> List[str]:
    """List object keys in a bucket/prefix."""
    response = minio_client().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    return [obj["Key"] for obj in response.get("Contents", [])]


def read_parquet_from_minio(bucket: str, key: str, **kwargs: Dict[str, Any]) -> pd.DataFrame:
    """Download one parquet object from MinIO and load it as a DataFrame."""
    buffer = io.BytesIO()
    minio_client().download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    return pd.read_parquet(buffer, **kwargs)


def postgres_url() -> str:
    """Build SQLAlchemy Postgres URL from warehouse environment variables."""
    host = os.getenv("WAREHOUSE_HOST", "warehouse")
    port = os.getenv("WAREHOUSE_PORT", "5432")
    database = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
    user = os.getenv("WAREHOUSE_USER", "admin")
    password = os.getenv("WAREHOUSE_PASSWORD", "admin")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def postgres_engine(echo: bool = False) -> Engine:
    """Create a reusable SQLAlchemy engine."""
    return create_engine(postgres_url(), pool_pre_ping=True, echo=echo)


def query_postgres(sql_query: str) -> pd.DataFrame:
    """Run a SQL query against warehouse Postgres and return a DataFrame."""
    with postgres_engine().connect() as conn:
        return pd.read_sql_query(text(sql_query), conn)
