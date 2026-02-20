import boto3
from botocore.exceptions import ClientError

s3 = boto3.resource('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='admin123',
    region_name='us-east-1'
)

buckets = ['bronze', 'silver', 'gold']

print("Creating MinIO buckets...")
for bucket_name in buckets:
    try:
        if s3.Bucket(bucket_name) not in s3.buckets.all():
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket already exists: {bucket_name}")
    except ClientError as e:
        print(f"Error checking/creating bucket {bucket_name}: {e}")
    except Exception as e:
         print(f"Error: {e}")
