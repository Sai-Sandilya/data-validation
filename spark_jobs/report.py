import argparse
import json
import sys
import os

# Add the backend directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from core.spark_session import get_spark
from core.db_utils import load_table
from config.db_config import SOURCE_DB, TARGET_DB

def generate_report(table=None):
    spark = get_spark()
    
    try:
        if table:
            tables = [table]
        else:
            # Get all tables from both databases
            # For now, return a mock report since we don't have actual table discovery
            tables = ["customers", "orders", "products"]
        
        reports = []
        for tbl in tables:
            try:
                src_df = load_table(spark, SOURCE_DB, tbl)
                tgt_df = load_table(spark, TARGET_DB, tbl)
                
                src_count = src_df.count()
                tgt_count = tgt_df.count()
                
                reports.append({
                    "table": tbl,
                    "source_count": src_count,
                    "target_count": tgt_count,
                    "difference": src_count - tgt_count,
                    "sync_status": "Pending" if src_count != tgt_count else "Synced"
                })
            except Exception as e:
                reports.append({
                    "table": tbl,
                    "error": str(e),
                    "source_count": 0,
                    "target_count": 0,
                    "difference": 0,
                    "sync_status": "Error"
                })
        
        spark.stop()
        return reports
        
    except Exception as e:
        spark.stop()
        return [{"error": f"Failed to generate report: {str(e)}"}]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sync report")
    parser.add_argument("--table", type=str, help="Specific table to report on")
    args = parser.parse_args()
    
    reports = generate_report(args.table)
    print(json.dumps(reports, indent=2))
