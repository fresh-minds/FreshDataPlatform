"""
Master script to export all gold layer data to the PostgreSQL warehouse.
"""

import os
import sys

# Ensure project root is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from shared.fabric.runtime import get_fabric_context, get_spark_session
from pipelines.odp_staffing_demand.export_to_warehouse import (
    export_all_to_warehouse,
)

def run_all_exports():
    spark = get_spark_session("MasterExport")
    notebookutils, fabric = get_fabric_context()
    
    print("\n" + "="*60)
    print("STARTING MASTER EXPORT TO WAREHOUSE")
    print("="*60 + "\n")
    
    print("--- 1. ODP Staffing Demand Export ---")
    export_all_to_warehouse(spark, notebookutils, fabric)
    
    print("\n" + "="*60)
    print("MASTER EXPORT COMPLETED")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_all_exports()
