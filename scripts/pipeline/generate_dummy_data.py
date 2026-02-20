import os
import sys
from datetime import datetime

from pyspark.sql import Row

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from shared.config.paths import LakehouseLayer, get_lakehouse_table_path
from shared.fabric.runtime import get_spark_session


def generate_dummy_job_market(spark):
    print("Generating dummy job market data...")

    rows = [
        Row(
            snapshot_date=datetime(2026, 1, 1),
            source="mock",
            vacancy_rate=3.2,
            vacancy_count=125000,
            generated_at=datetime(2026, 1, 1, 10, 0, 0),
        ),
        Row(
            snapshot_date=datetime(2026, 2, 1),
            source="mock",
            vacancy_rate=3.4,
            vacancy_count=129500,
            generated_at=datetime(2026, 2, 1, 10, 0, 0),
        ),
    ]

    df = spark.createDataFrame(rows)

    gold_path = get_lakehouse_table_path(
        table_name="it_market_snapshot",
        layer=LakehouseLayer.GOLD,
        domain="job_market_nl",
    )

    print(f"Writing {df.count()} records to {gold_path}...")
    df.write.format("delta").mode("overwrite").save(gold_path)
    print("Success: dummy data written to gold layer")


if __name__ == "__main__":
    spark = get_spark_session("DummyDataGen")
    generate_dummy_job_market(spark)
