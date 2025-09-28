from pyspark.sql.functions import sha2, concat_ws, col
from pyspark.sql import DataFrame

def add_hash_column(df: DataFrame) -> DataFrame:
    """Add a row_hash column (SHA-256 of all cols)."""
    return df.withColumn("row_hash", sha2(concat_ws("||", *[col(c) for c in df.columns]), 256))
