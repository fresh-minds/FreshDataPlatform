#!/usr/bin/env python3
"""
Refresh DataHub catalog to match current Postgres + MinIO state.

- Emits metadata for all current warehouse tables
- Emits metadata for all current MinIO objects
- Soft-deletes stale DataHub datasets that no longer exist
"""

import argparse
import logging
import os
import sys
from typing import Dict, Iterable, List, Set, Tuple

import boto3
from botocore.client import Config
from dotenv import load_dotenv
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Add repo root directory to path for imports when executed directly.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, REPO_ROOT)

from scripts.catalog.register_datahub_catalog import (
    create_dataset_urn,
    create_tags,
    get_warehouse_tables,
    register_table_in_datahub,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

# DataHub config
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://localhost:8081")
DATAHUB_TOKEN = os.getenv("DATAHUB_TOKEN")
DATAHUB_ENV = os.getenv("DATAHUB_ENV", "PROD")

# Warehouse config
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5433"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "open_data_platform_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")
MINIO_DATAHUB_PREFIX = os.getenv("MINIO_DATAHUB_PREFIX", "minio")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_minio_objects(s3_client) -> Iterable[Tuple[str, Dict]]:
    buckets = s3_client.list_buckets().get("Buckets", [])
    for bucket in buckets:
        bucket_name = bucket["Name"]
        continuation = None
        while True:
            params = {"Bucket": bucket_name}
            if continuation:
                params["ContinuationToken"] = continuation
            response = s3_client.list_objects_v2(**params)
            for obj in response.get("Contents", []) or []:
                key = obj.get("Key", "")
                if not key or key.endswith("/"):
                    continue
                yield bucket_name, obj
            if response.get("IsTruncated"):
                continuation = response.get("NextContinuationToken")
            else:
                break


def create_minio_dataset_urn(bucket: str, key: str) -> str:
    name = f"{MINIO_DATAHUB_PREFIX}.{bucket}/{key}"
    return f"urn:li:dataset:(urn:li:dataPlatform:s3,{name},{DATAHUB_ENV})"


def emit_minio_object(emitter: DatahubRestEmitter, bucket: str, obj: Dict) -> str:
    key = obj.get("Key")
    dataset_urn = create_minio_dataset_urn(bucket, key)

    last_modified = obj.get("LastModified")
    last_modified_str = last_modified.isoformat() if last_modified else ""

    props = DatasetPropertiesClass(
        name=f"{bucket}/{key}",
        description=f"MinIO object s3://{bucket}/{key}",
        customProperties={
            "source": "minio",
            "bucket": bucket,
            "key": key,
            "etag": str(obj.get("ETag", "")).strip('"'),
            "size_bytes": str(obj.get("Size", "")),
            "last_modified": last_modified_str,
        },
    )

    mcp = MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=props)
    emitter.emit(mcp)
    return dataset_urn


def build_expected_postgres_urns(tables: Dict[Tuple[str, str], Dict]) -> Set[str]:
    expected = set()
    for (schema_name, table_name) in tables.keys():
        expected.add(create_dataset_urn(schema_name, table_name))
    return expected


def build_expected_minio_urns(s3_client) -> Set[str]:
    expected = set()
    for bucket_name, obj in list_minio_objects(s3_client):
        expected.add(create_minio_dataset_urn(bucket_name, obj.get("Key")))
    return expected


def iter_catalog_datasets(graph: DataHubGraph) -> Iterable[str]:
    start = 0
    batch_size = 500
    while True:
        urns = graph.list_all_entity_urns("dataset", start=start, count=batch_size)
        if not urns:
            break
        for urn in urns:
            yield urn
        start += batch_size


def is_minio_dataset(urn: DatasetUrn) -> bool:
    return urn.platform.endswith(":s3") and urn.name.startswith(f"{MINIO_DATAHUB_PREFIX}.")


def cleanup_stale_datasets(
    graph: DataHubGraph,
    expected_postgres: Set[str],
    expected_minio: Set[str],
    dry_run: bool,
    hard_delete: bool,
    skip_minio: bool,
    skip_postgres: bool,
) -> Tuple[int, List[str]]:
    stale = []
    for urn_str in iter_catalog_datasets(graph):
        try:
            urn = DatasetUrn.from_string(urn_str)
        except Exception:
            continue

        if not skip_postgres and urn.platform.endswith(":postgres"):
            if urn_str not in expected_postgres:
                stale.append(urn_str)
            continue

        if not skip_minio and is_minio_dataset(urn):
            if urn_str not in expected_minio:
                stale.append(urn_str)
            continue

    if not stale:
        return 0, []

    for urn_str in stale:
        if dry_run:
            logger.info(f"[Dry-run] Would delete stale dataset: {urn_str}")
            continue
        if hard_delete:
            graph.hard_delete_entity(urn_str)
            logger.info(f"Hard-deleted stale dataset: {urn_str}")
        else:
            graph.soft_delete_entity(urn=urn_str, run_id="refresh-datahub-catalog")
            logger.info(f"Soft-deleted stale dataset: {urn_str}")

    return len(stale), stale


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh DataHub catalog from Postgres + MinIO")
    parser.add_argument("--dry-run", action="store_true", help="Only log deletions; do not delete")
    parser.add_argument("--hard-delete", action="store_true", help="Hard delete stale entities")
    parser.add_argument("--skip-minio", action="store_true", help="Skip MinIO sync + cleanup")
    parser.add_argument("--skip-postgres", action="store_true", help="Skip Postgres sync + cleanup")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Refreshing DataHub Catalog")
    logger.info("=" * 60)

    emitter = DatahubRestEmitter(DATAHUB_GMS_URL, token=DATAHUB_TOKEN)

    # Emit tags used by the warehouse tables
    if not args.skip_postgres:
        create_tags(emitter)

    expected_postgres: Set[str] = set()
    expected_minio: Set[str] = set()

    # Postgres sync
    if not args.skip_postgres:
        import psycopg2

        logger.info(f"Connecting to warehouse at {WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}")
        conn = psycopg2.connect(
            host=WAREHOUSE_HOST,
            port=WAREHOUSE_PORT,
            dbname=WAREHOUSE_DB,
            user=WAREHOUSE_USER,
            password=WAREHOUSE_PASSWORD,
        )
        tables = get_warehouse_tables(conn)
        logger.info(f"Found {len(tables)} warehouse tables/views")
        expected_postgres = build_expected_postgres_urns(tables)

        success = 0
        for (schema_name, table_name), table_info in tables.items():
            try:
                register_table_in_datahub(emitter, schema_name, table_name, table_info)
                success += 1
            except Exception as exc:
                logger.warning(f"Failed to emit table {schema_name}.{table_name}: {exc}")
        conn.close()
        logger.info(f"Emitted {success} warehouse datasets")

    # MinIO sync
    if not args.skip_minio:
        s3_client = get_s3_client()
        emitted = 0
        for bucket_name, obj in list_minio_objects(s3_client):
            try:
                emit_minio_object(emitter, bucket_name, obj)
                expected_minio.add(create_minio_dataset_urn(bucket_name, obj.get("Key")))
                emitted += 1
            except Exception as exc:
                logger.warning(f"Failed to emit MinIO object {bucket_name}/{obj.get('Key')}: {exc}")
        logger.info(f"Emitted {emitted} MinIO objects")

    # Cleanup stale datasets
    graph = DataHubGraph(DatahubClientConfig(server=DATAHUB_GMS_URL, token=DATAHUB_TOKEN))
    deleted_count, deleted_urns = cleanup_stale_datasets(
        graph,
        expected_postgres,
        expected_minio,
        dry_run=args.dry_run,
        hard_delete=args.hard_delete,
        skip_minio=args.skip_minio,
        skip_postgres=args.skip_postgres,
    )

    logger.info("=" * 60)
    if args.dry_run:
        logger.info(f"Dry-run complete. {deleted_count} stale datasets would be deleted.")
    else:
        logger.info(f"Cleanup complete. {deleted_count} stale datasets deleted.")
    if deleted_urns:
        logger.info("Stale datasets:")
        for urn in deleted_urns:
            logger.info(f"  - {urn}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
