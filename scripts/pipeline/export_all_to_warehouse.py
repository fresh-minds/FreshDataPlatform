"""
Master script to export all gold layer data to the PostgreSQL warehouse.
"""

import os
import sys

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from shared.fabric.runtime import get_fabric_context, get_spark_session
from pipelines.job_market_nl.export_to_warehouse import (
    run_export_job_market_to_warehouse as run_job_market_export,
)

def run_all_exports():
    spark = get_spark_session("MasterExport")
    notebookutils, fabric = get_fabric_context()
    
    print("\n" + "="*60)
    print("STARTING MASTER EXPORT TO WAREHOUSE")
    print("="*60 + "\n")
    
    print("--- 1. Job Market Export ---")
    run_job_market_export(spark, notebookutils, fabric)
    
    print("\n" + "="*60)
    print("MASTER EXPORT COMPLETED")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_all_exports()
