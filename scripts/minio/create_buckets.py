import os

import boto3
from botocore.exceptions import ClientError

endpoint_url = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
access_key = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY")
if not access_key or not secret_key:
    raise RuntimeError(
        "Missing MinIO credentials. Set MINIO_ROOT_USER/MINIO_ROOT_PASSWORD "
        "or MINIO_ACCESS_KEY/MINIO_SECRET_KEY."
    )

s3 = boto3.resource(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
)

buckets = ["bronze", "silver", "gold"]
existing_buckets = {bucket.name for bucket in s3.buckets.all()}

print("Creating MinIO buckets...")
for bucket_name in buckets:
    try:
        if bucket_name not in existing_buckets:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket already exists: {bucket_name}")
    except ClientError as e:
        print(f"Error checking/creating bucket {bucket_name}: {e}")
    except Exception as e:
        print(f"Error: {e}")
