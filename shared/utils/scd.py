from datetime import date
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    date_add,
    date_sub,
    lit,
    sha2,
    to_date,
    when,
)


def merge_scd2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_name: str,
    primary_key: str,
    pre_sql: Optional[str] = None,
) -> None:
    """
    Perform a Slowly Changing Dimension Type 2 (SCD2) merge.

    Args:
        spark: SparkSession
        source_df: The incoming dataframe (new data)
        target_table_name: The target Delta table name (fully qualified if needed)
        primary_key: The column name to use as the primary key
        pre_sql: Optional SQL to run before the merge (e.g. creating database)
    """
    # 1. Prepare Source
    # Hash column for change detection
    # We ignore 'loading_date' or other metadata columns if they exist in source
    cols_to_hash = [c for c in source_df.columns if c not in ["loading_date"]]
    
    source_df = source_df.withColumn(
        "hash", sha2(concat_ws("||", *[col(c).cast("string") for c in cols_to_hash]), 256)
    )

    # 2. Prepare Target
    # Check if target table exists
    table_exists = spark._jsparkSession.catalog().tableExists(target_table_name)
    
    if not table_exists:
        print(f"[SCD2] Table {target_table_name} does not exist. Creating initial load.")
        
        initial_df = source_df.withColumn("current_flag", lit(True)) \
            .withColumn("effective_date", current_timestamp()) \
            .withColumn("end_date", to_date(lit("9999-12-31")))
            
        initial_df.write.format("delta").mode("overwrite").saveAsTable(target_table_name)
        print(f"[SCD2] Initial load completed for {target_table_name}.")
        return

    target_df = spark.read.table(target_table_name)
    
    # 3. Logic for Merge
    # We follow a standard SCD2 pattern:
    # - Identify NEW records (INSERT)
    # - Identify CHANGED records (UPDATE old record + INSERT new record)
    # - Identify DELETED records (UPDATE old record to expire) - Optional, here we focus on updates
    #   (The original logic in the notebook seemed to handle deletes too, let's replicate the essence)
    
    # Filter for current records in target
    current_target = target_df.filter(col("current_flag") == True)
    
    # Join Source and Target on PK
    # We rename target cols to avoid conflict
    target_renamed = current_target.select(
        *[col(c).alias(f"tgt_{c}") for c in current_target.columns]
    )
    
    joined = source_df.join(
        target_renamed,
        source_df[primary_key] == target_renamed[f"tgt_{primary_key}"],
        how="full_outer"
    )
    
    # Determine Action
    # INSERT: Source PK exists, Target PK null
    # UPDATE: Source PK exists, Target PK exists, Hash different
    # DELETE: Source PK null, Target PK exists (and was current)
    # NOACTION: Hash same
    
    joined = joined.withColumn(
        "action",
        when(col(primary_key).isNotNull() & col(f"tgt_{primary_key}").isNull(), "INSERT")
        .when(
            col(primary_key).isNotNull()
            & col(f"tgt_{primary_key}").isNotNull()
            & (col("hash") != col("tgt_hash")),
            "UPDATE",
        )
        .when(col(primary_key).isNull() & col(f"tgt_{primary_key}").isNotNull(), "DELETE")
        .otherwise("NOACTION"),
    )
    
    # 4. Create the updates DataFrame
    
    # Records to be INSERTED (New or Updated)
    records_to_insert = joined.filter(col("action").isin("INSERT", "UPDATE")).select(
        source_df.columns + ["hash"]
    ).withColumn("current_flag", lit(True)) \
     .withColumn("effective_date", current_timestamp()) \
     .withColumn("end_date", to_date(lit("9999-12-31")))

     # Records to be EXPIRED (Updated or Deleted)
    # We need to grab the original target columns for these, but update the end_date and current_flag
    # Note: We must select the matching columns to union later.
    
    # For expired records, we want the ORIGINAL target data, just updated metadata.
    # But wait, Delta Merge is usually better for this.
    # However, standard PySpark generic SCD2 often uses Union ALL + Overwrite or Delta Merge.
    # The original notebook used a full overwrite approach (constructing the whole new history).
    # That is safer for small/medium tables but expensive for large ones.
    # Given the scale (likely small HR data), I can perform a Union approach similar to the original.
    
    # History (unchanged or already closed records)
    # We take everything from target that was NOT current OR was current but NOACTION
    # Actually simpler:
    # Take all non-current rows from target.
    # Take all current rows from target where action == NOACTION.
    # Take modified current rows (action == UPDATE or DELETE) with updated end_date.
    
    # Let's reconstruct the dataset:
    
    # 1. Historical records (already closed)
    history_df = target_df.filter(col("current_flag") == False)
    
    # 2. Unchanged current records
    unchanged_pks = joined.filter(col("action") == "NOACTION").select(f"tgt_{primary_key}")
    unchanged_df = current_target.join(unchanged_pks, current_target[primary_key] == unchanged_pks[f"tgt_{primary_key}"], "inner").drop(f"tgt_{primary_key}")
    
    # 3. Expiring records (Update or Delete)
    # We take the target properties, but set current=False, end_date = now
    expiring_df = joined.filter(col("action").isin("UPDATE", "DELETE")) \
        .select(*[col(f"tgt_{c}").alias(c) for c in target_df.columns]) \
        .withColumn("current_flag", lit(False)) \
        .withColumn("end_date", current_timestamp())
        
    # 4. New records (Insert or Update) - Created above as records_to_insert
    
    # Union all
    final_df = history_df.unionByName(unchanged_df).unionByName(expiring_df).unionByName(records_to_insert)
    
    # Write back
    final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table_name)
    print(f"[SCD2] Merge completed for {target_table_name}. Stats: {joined.groupBy('action').count().collect()}")
