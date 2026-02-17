from shared.fabric.runtime import get_spark_session
import os

spark = get_spark_session("VerifyMinIO")
try:
    path = "s3a://gold/job_market_nl/it_market_snapshot"
    print(f"Checking path: {path}")
    df = spark.read.format("delta").load(path)
    print(f"Successfully read {df.count()} records from {path}")
except Exception as e:
    print(f"FAILED to read path: {e}")

try:
    # Try listing buckets manually if possible
    # (Actually Spark doesn't have a direct 'list buckets' without hadoop fs)
    pass
except:
    pass
