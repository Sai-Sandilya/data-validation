AUDIT_COLS = [
    "insert_date", "update_date", "end_timestamp",
    "system_timestamp", "user_updated", "record_status",
    "created_at", "updated_at", "last_modified", "last_updated",
    "sync_status", "audit_timestamp", "created_by", "updated_by", 
    "sync_id", "batch_id", "row_hash",
    # SCD Type 2 columns
    "effective_date", "end_date", "is_current",
    "surrogate_id"  # Exclude auto-increment primary key from hash comparison
]

# Patterns for audit columns (columns ending with these suffixes)
AUDIT_COLUMN_PATTERNS = [
    "_previous",  # SCD Type 3 previous value columns
    "_audit", "_log", "_ts", "_timestamp", "_status", "_flag",
    "_created", "_updated", "_modified", "_synced", "_date"
]

NUM_PARTITIONS = 10
PARTITION_COLUMN = "id"
LOWER_BOUND = 1
UPPER_BOUND = 1000000000
