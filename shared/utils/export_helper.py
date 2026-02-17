from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType

def read_lakehouse_table(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a table from Lakehouse, trying Delta format first, then Parquet.
    """
    try:
        # Try Delta first
        df = spark.read.format("delta").load(path)
    except Exception as e:
        # Check if it's a "not a delta table" or "not found" error
        e_str = str(e)
        if "is not a Delta table" in e_str or "DELTA_TABLE_NOT_FOUND" in e_str or "NOT_A_DELTA_TABLE" in e_str:
            print(f"[Export] Path is not a Delta table, trying Parquet: {path}")
            df = spark.read.format("parquet").load(path)
        else:
            # Re-raise if it's something else (like path not found at all)
            raise e
    
    # Handle VOID columns (common in empty or null-only Parquet files)
    # Also handle NullType which Spark uses for "void"
    from pyspark.sql.types import NullType
    void_cols = [c.name for c in df.schema if isinstance(c.dataType, NullType)]
    if void_cols:
        print(f"[Export] Casting VOID columns to String: {void_cols}")
        for col_name in void_cols:
            df = df.withColumn(col_name, df[col_name].cast(StringType()))
            
    return df
