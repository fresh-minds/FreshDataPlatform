#!/usr/bin/env python3
"""Upload local fixture files into MinIO buckets (S3-compatible).

This is meant for local bootstrap/dev convenience so the MinIO buckets are not empty
after recreating Docker volumes.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv


def iter_files(root: Path, recursive: bool) -> Iterable[Path]:
    if recursive:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                yield path
        return

    for path in sorted(root.iterdir()):
        if path.is_file():
            yield path


def get_s3_client() -> "boto3.client":
    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

    # Prefer the credentials actually used by the MinIO container.
    # Many local scripts use MINIO_ACCESS_KEY/MINIO_SECRET_KEY, but those can drift.
    access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY") or "minioadmin"
    secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY") or "minioadmin"

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            # Could be auth error; re-raise for visibility.
            raise

    s3.create_bucket(Bucket=bucket)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload fixture files into MinIO")
    parser.add_argument("--fixtures-dir", required=True, help="Directory containing fixture files to upload")
    parser.add_argument("--bucket", default="bronze", help="MinIO bucket to upload into (default: bronze)")
    parser.add_argument("--prefix", default="bootstrap/fixtures", help="S3 key prefix to place files under")
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Upload files recursively (preserves relative subpaths under --fixtures-dir)",
    )
    args = parser.parse_args()

    root = Path(args.fixtures_dir).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise SystemExit(f"fixtures dir does not exist or is not a directory: {root}")

    s3 = get_s3_client()
    ensure_bucket(s3, args.bucket)

    uploaded = 0
    skipped = 0
    errors = 0

    for path in iter_files(root, args.recursive):
        rel = path.relative_to(root) if args.recursive else Path(path.name)
        key = f"{args.prefix.strip('/')}/{rel.as_posix()}"

        try:
            s3.upload_file(str(path), args.bucket, key)
            uploaded += 1
        except Exception as exc:  # noqa: BLE001 - best-effort bootstrap helper
            errors += 1
            print(f"[MinIO] Failed to upload {path} -> s3://{args.bucket}/{key}: {exc}")

    print(
        f"[MinIO] Upload complete: bucket={args.bucket} prefix={args.prefix} "
        f"uploaded={uploaded} skipped={skipped} errors={errors}"
    )

    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

