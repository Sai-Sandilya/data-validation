from core.spark_session import get_spark
from core.db_utils import load_table, write_table
from core.hash_utils import add_hash_column
from config.db_config import SOURCE_DB, TARGET_DB
from pyspark.sql.functions import lit

AUDIT_COLS = [
    "insert_date", "update_date", "end_timestamp",
    "system_timestamp", "user_updated", "record_status"
]

def perform_sync(table: str, src_config: dict = None, tgt_config: dict = None):
    if src_config is None:
        src_config = SOURCE_DB
    if tgt_config is None:
        tgt_config = TARGET_DB
    spark = get_spark()

    # Load tables
    src_df = load_table(spark, src_config, table)
    tgt_df = load_table(spark, tgt_config, table)

    # Exclude audit cols
    src_cols = [c for c in src_df.columns if c not in AUDIT_COLS]
    tgt_cols = [c for c in tgt_df.columns if c not in AUDIT_COLS]

    src_df = add_hash_column(src_df.select(src_cols))
    tgt_df = add_hash_column(tgt_df.select(tgt_cols))

    # Missing rows
    missing_rows = src_df.join(tgt_df, "row_hash", "left_anti")

    # Mismatched rows (PK assumed = first col)
    pk_col = src_cols[0]
    mismatched_rows = (
        src_df.alias("s")
        .join(tgt_df.alias("t"), on=[pk_col])
        .where("s.row_hash <> t.row_hash")
        .select("s.*")
        .withColumn("record_status", lit("SYNC"))
    )

    to_insert = missing_rows.unionByName(mismatched_rows, allowMissingColumns=True)

    if to_insert.count() > 0:
        write_table(to_insert, tgt_config, table)

    return f"✅ Sync complete for {table}. Missing={missing_rows.count()}, Mismatched={mismatched_rows.count()}"
