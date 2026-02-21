#!/usr/bin/env python3
"""
Utility script to clean up the data directory.
Deletes all contents of the data/ directory but keeps the directory itself.
"""
import os
import shutil
import glob
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

def clean_minio():
    """
    Clean all data from MinIO buckets.
    """
    print("\nAttempting to clean MinIO buckets...")
    
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    try:
        s3 = boto3.resource(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False  # MinIO usually uses self-signed certs or HTTP
        )
        
        # Check connection by listing buckets
        buckets = list(s3.buckets.all())
        print(f"Found {len(buckets)} MinIO buckets.")
        
        total_deleted = 0
        for bucket in buckets:
            print(f"Cleaning bucket: {bucket.name}")
            # Delete all objects in the bucket
            # s3.Bucket.objects.all().delete() returns a dict with Deleted key list
            deleted = bucket.objects.all().delete()
            
            count = 0
            if deleted and 'Deleted' in deleted[0]: # delete() returns a list of responses if multiple batches
                 # Simple approximation, or just rely on boto3 completing it
                 pass
            
            # Count objects to verify (optional, but good for logging)
            # Actually delete() is efficient.
            
            print(f"  Emptying bucket {bucket.name}...")
            
        print("MinIO cleanup complete.")
            
    except Exception as e:
        print(f"Failed to clean MinIO: {e}")
        print("Ensure MinIO is running (docker-compose up) logic if you want MinIO cleaned.")

def clean_data():
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    
    if not data_dir.exists():
        print(f"Data directory does not exist: {data_dir}")
    else:
        print(f"Cleaning data directory: {data_dir}")
        
        # Check if safe to delete (simple check)
        if not str(data_dir).endswith("data"):
            print("Safety check failed. Path does not end in 'data'. Aborting.")
            return

        # Delete all items in data/
        # We want to keep 'data/' itself if possible, or recreate it.
        count = 0
        for item in data_dir.iterdir():
            if item.name == ".gitkeep":
                continue
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                count += 1
                print(f"Deleted: {item.name}")
            except Exception as e:
                print(f"Failed to delete {item.name}: {e}")

        print(f"Local cleanup complete. {count} items removed.")

    # Also clean MinIO
    clean_minio()

if __name__ == "__main__":
    confirmation = input("Are you sure you want to delete all local data AND MinIO bucket contents? (y/N): ")
    if confirmation.lower() == "y":
        clean_data()
    else:
        print("Operation cancelled.")
