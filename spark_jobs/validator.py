import argparse
import json
import sys
import os

# Add the backend directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

from core.spark_session import get_spark
from core.db_utils import load_table, write_table
from core.hash_utils import add_hash_column
from config.db_config import SOURCE_DB, TARGET_DB
from config.settings import AUDIT_COLS
from pyspark.sql.functions import col, lit

def validate_and_sync(table, region=None):
    spark = get_spark()
    src_df = load_table(spark, SOURCE_DB, table)
    tgt_df = load_table(spark, TARGET_DB, table)

    if region:
        src_df = src_df.filter(col("region") == lit(region))
        tgt_df = tgt_df.filter(col("region") == lit(region))

    # Exclude audit columns
    src_cols = [c for c in src_df.columns if c not in AUDIT_COLS]
    tgt_cols = [c for c in tgt_df.columns if c not in AUDIT_COLS]

    src_h = add_hash_column(src_df.select(src_cols)).withColumnRenamed("row_hash", "src_hash")
    tgt_h = add_hash_column(tgt_df.select(tgt_cols)).withColumnRenamed("row_hash", "tgt_hash")

    # Join on the first column (assuming it's the primary key)
    pk_col = src_cols[0]
    joined = src_h.join(tgt_h, on=pk_col, how="outer")

    missing = joined.filter(col("tgt_hash").isNull()).select(*[col(c) for c in src_cols])
    mismatched = joined.filter((col("src_hash") != col("tgt_hash")) & col("tgt_hash").isNotNull()) \
                       .select(*[col(c) for c in src_cols])

    if missing.count() > 0:
        write_table(missing, TARGET_DB, table)

    if mismatched.count() > 0:
        mismatched = mismatched.withColumn("record_status", lit("SYNC"))
        write_table(mismatched, TARGET_DB, table)

    report = {
        "table": table,
        "rows_compared": joined.count(),
        "matched": joined.filter(col("src_hash") == col("tgt_hash")).count(),
        "missing_in_target": missing.count(),
        "mismatched": mismatched.count()
    }
    spark.stop()
    return report

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", type=str, required=True)
    parser.add_argument("--region", type=str, default=None)
    args = parser.parse_args()

    tables = args.tables.split(",")
    reports = []
    for t in tables:
        try:
            reports.append(validate_and_sync(t, args.region))
        except Exception as e:
            reports.append({
                "table": t,
                "error": str(e),
                "rows_compared": 0,
                "matched": 0,
                "missing_in_target": 0,
                "mismatched": 0
            })

    print(json.dumps(reports))