import os
import boto3
from botocore.client import Config
from atlas_client import AtlasService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO Config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "admin123")

def get_s3_client():
    return boto3.client('s3',
                        endpoint_url=MINIO_ENDPOINT,
                        aws_access_key_id=MINIO_ACCESS_KEY,
                        aws_secret_access_key=MINIO_SECRET_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1')

def ingest_minio_metadata():
    s3 = get_s3_client()
    atlas = AtlasService()

    # Create fs_path type if not exists (Simplified for demo)
    # In prod, we'd define a proper model. For now, we assume 'DataSet' or 'fs_path' exists or we map to 'DataSet'.
    # Default Atlas has 'hdfs_path' or 'aws_s3_bucket'. We'll use 'aws_s3_bucket' and 'aws_s3_object'.
    
    buckets = s3.list_buckets().get('Buckets', [])
    
    for bucket in buckets:
        bucket_name = bucket['Name']
        logger.info(f"Processing Bucket: {bucket_name}")
        
        # 1. Create Bucket Entity
        bucket_qn = f"s3://{bucket_name}@minio"
        bucket_entity = {
            "typeName": "aws_s3_bucket",
            "attributes": {
                "qualifiedName": bucket_qn,
                "name": bucket_name,
                "description": f"MinIO Bucket {bucket_name}",
                "owner": "admin"
            }
        }
        atlas.create_entity(bucket_entity)
        
        # 2. List Objects (Simplified: just top level or recursive)
        objects = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                key = obj['Key']
                obj_qn = f"s3://{bucket_name}/{key}@minio"
                
                # Check if it's a "folder" (ends with /) or file
                # Use aws_s3_object or aws_s3_pseudo_dir
                type_name = "aws_s3_pseudo_dir" if key.endswith('/') else "aws_s3_object"
                
                obj_entity = {
                    "typeName": type_name,
                    "attributes": {
                        "qualifiedName": obj_qn,
                        "name": key,
                        "objectPrefix": key,
                        "bucket": {"typeName": "aws_s3_bucket", "uniqueAttributes": {"qualifiedName": bucket_qn}},
                        "description": f"File in {bucket_name}"
                    }
                }
                
                # If file, add size/cleanup
                if type_name == "aws_s3_object":
                    obj_entity["attributes"]["dataType"] = key.split('.')[-1] if '.' in key else "unknown"
                    obj_entity["attributes"]["objectSize"] = obj['Size']

                try:
                    atlas.create_entity(obj_entity)
                except Exception as e:
                    logger.warning(f"Could not create entity for {key}: {e}")

if __name__ == "__main__":
    ingest_minio_metadata()
