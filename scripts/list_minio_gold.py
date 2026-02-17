
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "admin123"),
)

bucket_name = "gold"

print(f"Listing objects in bucket: {bucket_name}")
try:
    response = s3.list_objects_v2(Bucket=bucket_name)
    if "Contents" in response:
        for obj in response["Contents"]:
            print(f" - {obj['Key']}")
    else:
        print("Bucket is empty.")
except Exception as e:
    print(f"Error: {e}")
