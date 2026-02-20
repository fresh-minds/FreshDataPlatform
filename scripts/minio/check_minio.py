import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def list_minio_objects():
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'admin'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'admin123'),
        region_name='us-east-1'
    )
    
    buckets = ['bronze', 'silver', 'gold']
    for bucket in buckets:
        print(f"\n--- Bucket: {bucket} ---")
        try:
            response = s3.list_objects_v2(Bucket=bucket)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"  {obj['Key']}")
            else:
                print("  (Empty)")
        except Exception as e:
            print(f"  Error: {e}")

if __name__ == "__main__":
    list_minio_objects()
