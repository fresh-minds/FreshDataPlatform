"""MinIO (S3-compatible) bronze storage utilities.

Bronze path layout:
  bronze/{source_name}/dataset={dataset}/run_dt=YYYY-MM-DD/raw/{artifact_id}.{ext}
  bronze/{source_name}/dataset={dataset}/run_dt=YYYY-MM-DD/meta/{artifact_id}.json

Credentials are resolved in priority order:
  1. Airflow connection object passed as `conn`
  2. Environment variables: MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""
from __future__ import annotations

import json
import logging
import os
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from .provenance import ArtifactMeta

log = logging.getLogger(__name__)

_RAW_KEY_TPL = (
    "bronze/{source_name}/dataset={dataset}"
    "/run_dt={run_dt}/raw/{artifact_id}.{ext}"
)
_META_KEY_TPL = (
    "bronze/{source_name}/dataset={dataset}"
    "/run_dt={run_dt}/meta/{artifact_id}.json"
)


def _build_client(conn=None):
    """Build a boto3 S3 client from an Airflow connection or env vars."""
    if conn is not None:
        endpoint_url = getattr(conn, "host", None) or os.environ.get(
            "MINIO_ENDPOINT", "http://minio:9000"
        )
        # Airflow HttpHook-style extra JSON may carry endpoint_url override
        extra = getattr(conn, "extra_dejson", {}) or {}
        endpoint_url = extra.get("endpoint_url", endpoint_url)
        access_key = conn.login or os.environ.get("MINIO_ACCESS_KEY", "")
        secret_key = conn.password or os.environ.get("MINIO_SECRET_KEY", "")
    else:
        endpoint_url = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "")

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def ensure_bucket(bucket: str, conn=None) -> None:
    """Create *bucket* if it does not exist. Idempotent."""
    client = _build_client(conn)
    try:
        client.head_bucket(Bucket=bucket)
        log.debug("Bucket '%s' already exists.", bucket)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            client.create_bucket(Bucket=bucket)
            log.info("Created MinIO bucket '%s'.", bucket)
        else:
            raise


def write_bronze_artifact(
    *,
    bucket: str,
    run_dt: str,
    raw_bytes: bytes,
    meta: ArtifactMeta,
    extension: str,
    conn=None,
) -> tuple[str, str]:
    """Write raw artifact bytes and paired meta JSON to MinIO.

    Returns:
        (raw_key, meta_key) — both relative to the bucket root.
    """
    client = _build_client(conn)

    raw_key = _RAW_KEY_TPL.format(
        source_name=meta.source_name,
        dataset=meta.dataset,
        run_dt=run_dt,
        artifact_id=meta.artifact_id,
        ext=extension,
    )
    meta_key = _META_KEY_TPL.format(
        source_name=meta.source_name,
        dataset=meta.dataset,
        run_dt=run_dt,
        artifact_id=meta.artifact_id,
    )

    client.put_object(Bucket=bucket, Key=raw_key, Body=raw_bytes)
    log.info("Bronze raw → s3://%s/%s  (%d bytes)", bucket, raw_key, len(raw_bytes))

    meta_bytes = meta.to_json().encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=meta_key,
        Body=meta_bytes,
        ContentType="application/json",
    )
    log.info("Bronze meta → s3://%s/%s", bucket, meta_key)

    return raw_key, meta_key


def list_bronze_meta_for_run(
    *,
    bucket: str,
    source_name: str,
    dataset: str,
    run_dt: str,
    conn=None,
) -> list[dict]:
    """Return parsed meta JSON dicts for all artifacts in a given run partition."""
    client = _build_client(conn)
    prefix = (
        f"bronze/{source_name}/dataset={dataset}/run_dt={run_dt}/meta/"
    )
    paginator = client.get_paginator("list_objects_v2")
    metas: list[dict] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            metas.append(json.loads(body))
    return metas


def read_bronze_raw(*, bucket: str, key: str, conn=None) -> bytes:
    """Fetch a raw artifact from MinIO by its S3 key."""
    client = _build_client(conn)
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()
