from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import mysql.connector
from mysql.connector import Error
from core.hash_utils import add_hash_column
from core.spark_session import get_spark
from core.db_utils import load_table
from config.db_config import SOURCE_DB, TARGET_DB
from config.settings import AUDIT_COLS, AUDIT_COLUMN_PATTERNS

router = APIRouter()

class ComparisonRequest(BaseModel):
    table: str
    region: Optional[str] = None

class ComparisonResult(BaseModel):
    table: str
    source_count: int
    target_count: int
    matched_count: int
    unmatched_count: int
    missing_in_target: int
    extra_in_target: int
    status: str

class DetailedRecord(BaseModel):
    record_id: Any
    source_data: Optional[Dict[str, Any]]
    target_data: Optional[Dict[str, Any]]
    source_hash: Optional[str]
    target_hash: Optional[str]
    difference_type: str  # 'missing', 'extra', 'mismatch'
    differences: List[str]

class SyncRequest(BaseModel):
    table: str
    selected_records: List[Any]
    scd_type: Optional[str] = "SCD3"  # Default to SCD Type 3

@router.post("/compare-table", response_model=ComparisonResult)
def compare_table(request: ComparisonRequest):
    """Compare a table between source and target databases"""
    try:
        # Get data using direct MySQL connection for now
        source_data = get_table_data(SOURCE_DB, request.table, request.region)
        target_data = get_table_data(TARGET_DB, request.table, request.region)
        
        # Calculate hash for each record
        source_with_hash = add_hash_to_records(source_data)
        target_with_hash = add_hash_to_records(target_data)
        
        # Handle SCD2 tables specially (compare by business_id)
        if request.table.endswith('_scd2') and source_with_hash and 'business_id' in source_with_hash[0]:
            comparison = compare_scd2_records(source_with_hash, target_with_hash)
        else:
            # Regular table comparison
            comparison = compare_records(source_with_hash, target_with_hash)
        
        return ComparisonResult(
            table=request.table,
            source_count=len(source_data),
            target_count=len(target_data),
            matched_count=comparison['matched'],
            unmatched_count=comparison['unmatched'],
            missing_in_target=comparison['missing'],
            extra_in_target=comparison['extra'],
            status="Completed"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")

@router.post("/debug-hash-comparison")
def debug_hash_comparison(request: ComparisonRequest):
    """Debug hash comparison to see what's being compared"""
    try:
        source_data = get_table_data(SOURCE_DB, request.table, request.region)
        target_data = get_table_data(TARGET_DB, request.table, request.region)
        
        # Take first record from each for debugging
        if source_data and target_data:
            source_record = source_data[0]
            target_record = target_data[0]
            
            # Show what columns are being included/excluded
            source_business_data = {}
            target_business_data = {}
            excluded_columns = []
            
            for k, v in source_record.items():
                if k in AUDIT_COLS:
                    excluded_columns.append(f"{k} (in AUDIT_COLS)")
                    continue
                    
                is_audit_pattern = any(k.endswith(pattern) for pattern in AUDIT_COLUMN_PATTERNS)
                if is_audit_pattern:
                    excluded_columns.append(f"{k} (matches pattern)")
                    continue
                    
                source_business_data[k] = v
            
            for k, v in target_record.items():
                if k in AUDIT_COLS:
                    continue
                    
                is_audit_pattern = any(k.endswith(pattern) for pattern in AUDIT_COLUMN_PATTERNS)
                if is_audit_pattern:
                    continue
                    
                target_business_data[k] = v
            
            # Calculate hashes
            import hashlib
            import json
            
            source_str = json.dumps(source_business_data, sort_keys=True, default=str)
            target_str = json.dumps(target_business_data, sort_keys=True, default=str)
            
            source_hash = hashlib.sha256(source_str.encode()).hexdigest()
            target_hash = hashlib.sha256(target_str.encode()).hexdigest()
            
            return {
                "source_business_data": source_business_data,
                "target_business_data": target_business_data,
                "excluded_columns": excluded_columns,
                "source_hash": source_hash,
                "target_hash": target_hash,
                "hashes_match": source_hash == target_hash,
                "source_string": source_str,
                "target_string": target_str
            }
        
        return {"error": "No data found"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")

@router.post("/detailed-comparison")
def get_detailed_comparison(request: ComparisonRequest):
    """Get detailed record-by-record comparison"""
    try:
        source_data = get_table_data(SOURCE_DB, request.table, request.region)
        target_data = get_table_data(TARGET_DB, request.table, request.region)
        
        source_with_hash = add_hash_to_records(source_data)
        target_with_hash = add_hash_to_records(target_data)
        
        # Handle SCD2 tables specially (compare by business_id)
        if request.table.endswith('_scd2') and source_with_hash and 'business_id' in source_with_hash[0]:
            detailed_records = get_scd2_detailed_differences(source_with_hash, target_with_hash)
        else:
            detailed_records = get_detailed_differences(source_with_hash, target_with_hash)
        
        return {
            "table": request.table,
            "total_differences": len(detailed_records),
            "records": detailed_records[:50]  # Limit to first 50 for performance
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detailed comparison failed: {str(e)}")

def get_table_data(db_config: dict, table: str, region: Optional[str] = None):
    """Get table data from database"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=int(db_config["port"]),
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connection_timeout=10,
        )
        
        cursor = connection.cursor(dictionary=True)
        
        # Build query
        query = f"SELECT * FROM {table}"
        params = []
        
        if region:
            query += " WHERE region = %s"
            params.append(region)
            
        query += " LIMIT 1000"  # Limit for performance
        
        cursor.execute(query, params)
        records = cursor.fetchall()
        cursor.close()
        
        return records
        
    except Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
    finally:
        if connection and connection.is_connected():
            connection.close()

def add_hash_to_records(records: List[Dict]) -> List[Dict]:
    """Add hash column to records - EXCLUDE ALL AUDIT COLUMNS"""
    import hashlib
    import json
    
    for record in records:
        # Exclude audit columns from hash calculation
        hash_data = {}
        
        for k, v in record.items():
            # Skip if column is in AUDIT_COLS list
            if k in AUDIT_COLS:
                continue
                
            # Skip if column matches audit patterns
            is_audit_pattern = any(k.endswith(pattern) for pattern in AUDIT_COLUMN_PATTERNS)
            if is_audit_pattern:
                continue
                
            # Include only business data columns
            hash_data[k] = v
        
        # Create hash from business data only
        record_str = json.dumps(hash_data, sort_keys=True, default=str)
        record_hash = hashlib.sha256(record_str.encode()).hexdigest()
        record['row_hash'] = record_hash
        
    return records

def compare_records(source_records: List[Dict], target_records: List[Dict]) -> Dict:
    """Compare source and target records"""
    # Assuming first column is the primary key
    if not source_records and not target_records:
        return {'matched': 0, 'unmatched': 0, 'missing': 0, 'extra': 0}
    
    # Get primary key column (assume first column)
    pk_column = list(source_records[0].keys())[0] if source_records else list(target_records[0].keys())[0]
    
    # Create lookup dictionaries
    source_lookup = {record[pk_column]: record for record in source_records}
    target_lookup = {record[pk_column]: record for record in target_records}
    
    matched = 0
    unmatched = 0
    missing = 0
    extra = 0
    
    # Check source records
    for pk, source_record in source_lookup.items():
        if pk in target_lookup:
            target_record = target_lookup[pk]
            if source_record['row_hash'] == target_record['row_hash']:
                matched += 1
            else:
                unmatched += 1
        else:
            missing += 1
    
    # Check for extra records in target
    for pk in target_lookup:
        if pk not in source_lookup:
            extra += 1
    
    return {
        'matched': matched,
        'unmatched': unmatched,
        'missing': missing,
        'extra': extra
    }

def compare_scd2_records(source_records: List[Dict], target_records: List[Dict]) -> Dict:
    """Compare SCD2 records by business_id (only current target records)"""
    if not source_records and not target_records:
        return {'matched': 0, 'unmatched': 0, 'missing': 0, 'extra': 0}
    
    # For SCD2, create lookups by business_id
    source_lookup = {record['business_id']: record for record in source_records}
    
    # For target, only get current records (is_current = True/1)
    current_target_records = [
        record for record in target_records 
        if record.get('is_current', False) in [True, 1, '1']
    ]
    target_lookup = {record['business_id']: record for record in current_target_records}
    
    # Get all extra (non-current) records in target for counting
    all_target_business_ids = set(record['business_id'] for record in target_records)
    current_target_business_ids = set(record['business_id'] for record in current_target_records)
    
    matched = 0
    unmatched = 0
    missing = 0
    extra = 0
    
    # Check source records against current target records
    for business_id, source_record in source_lookup.items():
        if business_id in target_lookup:
            target_record = target_lookup[business_id]
            # Compare hashes (excluding SCD2 system columns)
            if source_record['row_hash'] == target_record['row_hash']:
                matched += 1
            else:
                unmatched += 1
        else:
            missing += 1
    
    # Check for extra business entities in target (that don't exist in source)
    for business_id in all_target_business_ids:
        if business_id not in source_lookup:
            extra += 1
    
    return {
        'matched': matched,
        'unmatched': unmatched,
        'missing': missing,
        'extra': extra
    }

def get_detailed_differences(source_records: List[Dict], target_records: List[Dict]) -> List[DetailedRecord]:
    """Get detailed differences between records"""
    if not source_records and not target_records:
        return []
    
    # Get primary key column
    pk_column = list(source_records[0].keys())[0] if source_records else list(target_records[0].keys())[0]
    
    # Create lookup dictionaries
    source_lookup = {record[pk_column]: record for record in source_records}
    target_lookup = {record[pk_column]: record for record in target_records}
    
    detailed_records = []
    
    # Check source records
    for pk, source_record in source_lookup.items():
        if pk in target_lookup:
            target_record = target_lookup[pk]
            if source_record['row_hash'] != target_record['row_hash']:
                # Find differences
                differences = []
                for key in source_record:
                    if key != 'row_hash' and key in target_record:
                        if source_record[key] != target_record[key]:
                            differences.append(f"{key}: '{source_record[key]}' vs '{target_record[key]}'")
                
                detailed_records.append(DetailedRecord(
                    record_id=pk,
                    source_data=source_record,
                    target_data=target_record,
                    source_hash=source_record['row_hash'],
                    target_hash=target_record['row_hash'],
                    difference_type='mismatch',
                    differences=differences
                ))
        else:
            # Missing in target
            detailed_records.append(DetailedRecord(
                record_id=pk,
                source_data=source_record,
                target_data=None,
                source_hash=source_record['row_hash'],
                target_hash=None,
                difference_type='missing',
                differences=['Record missing in target database']
            ))
    
    # Check for extra records in target
    for pk, target_record in target_lookup.items():
        if pk not in source_lookup:
            detailed_records.append(DetailedRecord(
                record_id=pk,
                source_data=None,
                target_data=target_record,
                source_hash=None,
                target_hash=target_record['row_hash'],
                difference_type='extra',
                differences=['Extra record in target database']
            ))
    
    return detailed_records

def get_scd2_detailed_differences(source_records: List[Dict], target_records: List[Dict]) -> List[DetailedRecord]:
    """Get detailed differences for SCD2 tables (compare by business_id)"""
    if not source_records and not target_records:
        return []
    
    # For SCD2, compare by business_id
    source_lookup = {record['business_id']: record for record in source_records}
    
    # For target, only compare against current records (is_current = True/1)
    current_target_records = [
        record for record in target_records 
        if record.get('is_current', False) in [True, 1, '1']
    ]
    target_lookup = {record['business_id']: record for record in current_target_records}
    
    # Get all extra business IDs in target (that don't exist in source)
    all_target_business_ids = set(record['business_id'] for record in target_records)
    extra_business_ids = all_target_business_ids - set(source_lookup.keys())
    
    detailed_records = []
    
    # Process source records
    for business_id, source_record in source_lookup.items():
        target_record = target_lookup.get(business_id)
        
        if target_record:
            # Business entity exists in both, check for differences
            if source_record['row_hash'] != target_record['row_hash']:
                # Find differences
                differences = []
                for key in source_record:
                    if key not in ['row_hash', 'surrogate_id', 'effective_date', 'end_date', 'is_current', 'record_status', 'created_at']:
                        if key in target_record and source_record[key] != target_record[key]:
                            differences.append(f"{key}: '{source_record[key]}' vs '{target_record[key]}'")
                
                detailed_records.append(DetailedRecord(
                    record_id=business_id,
                    source_data=source_record,
                    target_data=target_record,
                    source_hash=source_record['row_hash'],
                    target_hash=target_record['row_hash'],
                    difference_type='mismatch',
                    differences=differences
                ))
        else:
            # Missing in target (new business entity)
            detailed_records.append(DetailedRecord(
                record_id=business_id,
                source_data=source_record,
                target_data=None,
                source_hash=source_record['row_hash'],
                target_hash=None,
                difference_type='missing',
                differences=['Record missing in target database']
            ))
    
    # Process extra business entities in target (that don't exist in source)
    for business_id in extra_business_ids:
        # Get the current record for this business entity
        extra_record = next(
            (record for record in current_target_records if record['business_id'] == business_id), 
            None
        )
        if extra_record:
            detailed_records.append(DetailedRecord(
                record_id=business_id,
                source_data=None,
                target_data=extra_record,
                source_hash=None,
                target_hash=extra_record['row_hash'],
                difference_type='extra',
                differences=['Extra record in target database']
            ))
    
    return detailed_records

@router.post("/sync-selected-records")
def sync_selected_records(request: SyncRequest):
    """Sync selected records from source to target database with comprehensive logging"""
    import time
    from datetime import datetime
    
    start_time = time.time()
    sync_id = f"SYNC_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    
    try:
        # Get source and target data for comparison
        source_data = get_table_data(SOURCE_DB, request.table)
        target_data = get_table_data(TARGET_DB, request.table)
        
        # Filter selected records
        selected_source_records = [
            record for record in source_data 
            if record[get_primary_key_column(source_data)] in request.selected_records
        ]
        
        if not selected_source_records:
            return {"message": "No records found to sync", "synced_count": 0}
        
        # Prepare audit data
        audit_details = []
        pk_column = get_primary_key_column(selected_source_records)
        target_lookup = {record[pk_column]: record for record in target_data}
        
        # Analyze each selected record for audit
        for source_record in selected_source_records:
            pk_value = source_record[pk_column]
            existing_target = target_lookup.get(pk_value)
            
            if existing_target:
                # Record exists - will be SCD Type 3 update
                changes = []
                old_values = {}
                new_values = {}
                
                for col, new_val in source_record.items():
                    if col != 'row_hash' and col in existing_target:
                        old_val = existing_target[col]
                        if old_val != new_val:
                            changes.append(f"{col}: '{old_val}' -> '{new_val}'")
                            old_values[col] = old_val
                            new_values[col] = new_val
                
                audit_details.append({
                    "record_id": pk_value,
                    "operation_type": "unmatched_scd3",
                    "old_values": old_values,
                    "new_values": new_values,
                    "changes_made": changes,
                    "error_message": None
                })
            else:
                # New record - will be inserted
                audit_details.append({
                    "record_id": pk_value,
                    "operation_type": "missing_insert",
                    "old_values": None,
                    "new_values": {k: v for k, v in source_record.items() if k != 'row_hash'},
                    "changes_made": ["New record inserted"],
                    "error_message": None
                })
        
        # Perform the actual sync with SCD strategy
        synced_count = insert_records_to_target(request.table, selected_source_records, request.scd_type)
        
        # Calculate processing time
        processing_time = time.time() - start_time
        
        # Create comprehensive audit log
        create_audit_log(
            sync_id=sync_id,
            table_name=request.table,
            source_count=len(source_data),
            target_count=len(target_data),
            selected_records=request.selected_records,
            audit_details=audit_details,
            processing_time=processing_time,
            synced_count=synced_count
        )
        
        return {
            "message": f"Successfully synced {synced_count} records to target database",
            "synced_count": synced_count,
            "table": request.table,
            "sync_id": sync_id,
            "processing_time": round(processing_time, 3),
            "audit_created": True
        }
        
    except Exception as e:
        # Log error in audit system
        processing_time = time.time() - start_time
        try:
            create_error_audit_log(sync_id, request.table, str(e), processing_time)
        except:
            pass  # Don't fail if audit logging fails
        
        raise HTTPException(status_code=500, detail=f"Sync failed: {str(e)}")

def get_table_columns(connection, table: str) -> List[str]:
    """Get column names for a table"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"DESCRIBE {table}")
        columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return columns
    except:
        return []

def get_primary_key_column(records: List[Dict]) -> str:
    """Get the primary key column name - handle SCD2 tables specially"""
    if not records:
        return "id"
    
    # For SCD2 tables, use business_id as the logical key for comparisons
    if 'business_id' in records[0]:
        return "business_id"
    
    # For regular tables, assume first column or look for 'id'
    if 'id' in records[0]:
        return "id"
    
    return list(records[0].keys())[0]

def insert_records_to_target(table: str, records: List[Dict], scd_type: str = "SCD3"):
    """Insert/Update records into target database with SCD Type 3 approach"""
    if not records:
        return 0
    
    connection = None
    try:
        connection = mysql.connector.connect(
            host=TARGET_DB["host"],
            port=int(TARGET_DB["port"]),
            database=TARGET_DB["database"],
            user=TARGET_DB["user"],
            password=TARGET_DB["password"],
            connection_timeout=10,
        )
        
        cursor = connection.cursor(dictionary=True)
        
        # Ensure appropriate SCD columns exist based on type
        if scd_type == "SCD2":
            ensure_scd2_columns(cursor, table, records)
        else:  # SCD3
            ensure_scd_columns(cursor, table, records)  # existing SCD3 logic
        connection.commit()
        
        synced_count = 0
        pk_column = get_primary_key_column(records)
        
        for record in records:
            # Remove the row_hash if it exists (it's calculated, not stored)
            record_copy = {k: v for k, v in record.items() if k != 'row_hash'}
            pk_value = record_copy[pk_column]
            
            # Check if record exists in target
            if scd_type == "SCD2" and 'is_current' in get_table_columns(connection, table):
                # For SCD2 tables, look for current record
                cursor.execute(f"SELECT * FROM {table} WHERE {pk_column} = %s AND is_current = TRUE", (pk_value,))
            else:
                # For regular tables, simple lookup
                cursor.execute(f"SELECT * FROM {table} WHERE {pk_column} = %s", (pk_value,))
            
            existing_record = cursor.fetchone()
            
            if existing_record:
                if scd_type == "SCD2":
                    # Case 2: UNMATCHED - SCD Type 2 Update (create new row, expire old)
                    synced_count += perform_scd_type2_update(cursor, table, record_copy, existing_record, pk_column, pk_value)
                else:
                    # Case 2: UNMATCHED - SCD Type 3 Update
                    synced_count += perform_scd_type3_update(cursor, table, record_copy, existing_record, pk_column, pk_value)
            else:
                # Case 1: MISSING - Simple Insert (same for both SCD types)
                synced_count += perform_simple_insert(cursor, table, record_copy, scd_type)
        
        connection.commit()
        cursor.close()
        
        return synced_count
        
    except Error as e:
        if connection:
            connection.rollback()
        raise HTTPException(status_code=400, detail=f"Database sync error: {str(e)}")
    finally:
        if connection and connection.is_connected():
            connection.close()

def ensure_scd_columns(cursor, table: str, records: List[Dict]):
    """Ensure SCD Type 3 columns exist for all data columns"""
    if not records:
        return
    
    # Get sample record to determine columns
    sample_record = records[0]
    data_columns = [col for col in sample_record.keys() if col not in ['row_hash']]
    
    # Add record_status column
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN record_status VARCHAR(50) DEFAULT 'ACTIVE'")
    except mysql.connector.Error:
        pass
    
    # Add last_updated column
    try:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    except mysql.connector.Error:
        pass
    
    # Add _previous columns for each data column (except primary key)
    pk_column = list(sample_record.keys())[0]  # Assume first column is PK
    
    for col in data_columns:
        if col != pk_column and col not in ['record_status', 'last_updated']:
            try:
                # Get column type from original column
                cursor.execute(f"SHOW COLUMNS FROM {table} LIKE '{col}'")
                col_info = cursor.fetchone()
                if col_info:
                    col_type = col_info['Type']
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col}_previous {col_type}")
            except mysql.connector.Error:
                pass

def perform_scd_type3_update(cursor, table: str, new_record: Dict, existing_record: Dict, pk_column: str, pk_value):
    """Perform SCD Type 3 update - preserve old values in _previous columns"""
    update_parts = []
    values = []
    
    # Compare each field and build update statement
    for col, new_value in new_record.items():
        if col == pk_column:
            continue  # Skip primary key
            
        if col in existing_record:
            old_value = existing_record[col]
            
            # If value changed, preserve old value and update new value
            if old_value != new_value:
                # Set previous value
                prev_col = f"{col}_previous"
                if prev_col in existing_record:  # Column exists
                    update_parts.append(f"{prev_col} = %s")
                    values.append(old_value)
                
                # Set new value
                update_parts.append(f"{col} = %s")
                values.append(new_value)
        else:
            # New column, just set the value
            update_parts.append(f"{col} = %s")
            values.append(new_value)
    
    # Always update these tracking columns
    update_parts.extend([
        "record_status = %s",
        "last_updated = NOW()"
    ])
    values.append('UPDATED')
    
    if update_parts:
        update_query = f"""
            UPDATE {table} 
            SET {', '.join(update_parts)}
            WHERE {pk_column} = %s
        """
        values.append(pk_value)
        
        cursor.execute(update_query, values)
        return cursor.rowcount
    
    return 0

def ensure_scd2_columns(cursor, table: str, records: List[Dict]):
    """Ensure SCD Type 2 columns exist (effective_date, end_date, is_current)"""
    try:
        # Add effective_date column
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN effective_date DATETIME DEFAULT CURRENT_TIMESTAMP")
    except mysql.connector.Error:
        pass
    
    try:
        # Add end_date column
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN end_date DATETIME DEFAULT '9999-12-31 23:59:59'")
    except mysql.connector.Error:
        pass
    
    try:
        # Add is_current flag
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN is_current BOOLEAN DEFAULT TRUE")
    except mysql.connector.Error:
        pass

def perform_scd_type2_update(cursor, table: str, new_record: Dict, existing_record: Dict, pk_column: str, pk_value):
    """Perform TRUE SCD Type 2 update - create new row and expire old one"""
    from datetime import datetime
    
    # Check if there are actual changes
    changes_detected = False
    for col, new_value in new_record.items():
        if col == pk_column or col in ['surrogate_id', 'effective_date', 'end_date', 'is_current', 'record_status', 'created_at']:
            continue  # Skip system columns
        if col in existing_record and existing_record[col] != new_value:
            changes_detected = True
            break
    
    if not changes_detected:
        return 0  # No changes, nothing to sync
    
    current_time = datetime.now()
    
    # Step 1: Expire the current record(s) for this business entity
    expire_query = f"""
        UPDATE {table} 
        SET end_date = %s, is_current = FALSE, record_status = 'EXPIRED'
        WHERE {pk_column} = %s AND (is_current IS NULL OR is_current = TRUE)
    """
    cursor.execute(expire_query, (current_time, pk_value))
    
    # Step 2: Insert new record with current data (TRUE SCD2 - creates new row)
    # Filter out system columns that should not be copied
    business_columns = {k: v for k, v in new_record.items() 
                       if k not in ['surrogate_id', 'row_hash']}
    
    # Add SCD2 system columns
    business_columns.update({
        'effective_date': current_time,
        'end_date': datetime.strptime('9999-12-31 23:59:59', '%Y-%m-%d %H:%M:%S'),
        'is_current': True,
        'record_status': 'CURRENT'
    })
    
    # Build insert query
    columns = list(business_columns.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({placeholders})
    """
    
    values = [business_columns[col] for col in columns]
    cursor.execute(insert_query, values)
    
    return cursor.rowcount

def perform_simple_insert(cursor, table: str, record: Dict, scd_type: str = "SCD3"):
    """Perform simple insert for missing records"""
    from datetime import datetime
    
    # Create a copy and filter out system columns that shouldn't be inserted
    if scd_type == "SCD2":
        # For SCD2, exclude surrogate_id (auto-increment) and other system columns
        record_copy = {k: v for k, v in record.items() 
                      if k not in ['surrogate_id', 'row_hash', 'created_at']}
        
        # Add SCD2 specific fields
        record_copy.update({
            'effective_date': datetime.now(),
            'end_date': datetime.strptime('9999-12-31 23:59:59', '%Y-%m-%d %H:%M:%S'),
            'is_current': True,
            'record_status': 'CURRENT'
        })
    else:
        # For SCD3 tables, just exclude row_hash
        record_copy = {k: v for k, v in record.items() if k != 'row_hash'}
        
        # Add SCD3 specific fields
        record_copy.update({
            'record_status': 'UPDATED',
            'last_updated': datetime.now()
        })
    
    # Get column names and values
    columns = list(record_copy.keys())
    values = list(record_copy.values())
    
    # Create placeholders for the query
    placeholders = ['%s'] * len(values)
    
    insert_query = f"""
        INSERT INTO {table} ({', '.join(columns)}) 
        VALUES ({', '.join(placeholders)})
    """
    
    cursor.execute(insert_query, values)
    return cursor.rowcount

def create_audit_log(sync_id: str, table_name: str, source_count: int, target_count: int, 
                    selected_records: List, audit_details: List, processing_time: float, synced_count: int):
    """Create comprehensive audit log for sync operation"""
    from datetime import datetime
    import json
    
    try:
        # Ensure audit tables exist
        ensure_audit_tables()
        
        # Count operations by type
        matched_count = len([d for d in audit_details if d['operation_type'] == 'matched'])
        unmatched_count = len([d for d in audit_details if d['operation_type'] == 'unmatched_scd3'])
        missing_count = len([d for d in audit_details if d['operation_type'] == 'missing_insert'])
        
        # Insert main audit log
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO sync_audit_log 
            (sync_id, table_name, sync_timestamp, total_source_records, 
             total_target_records, matched_count, unmatched_count, 
             missing_count, extra_count, synced_records, 
             processing_time_seconds, user_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            sync_id,
            table_name,
            datetime.now(),
            source_count,
            target_count,
            matched_count,
            unmatched_count,
            missing_count,
            0,  # extra_count
            json.dumps(selected_records),
            processing_time,
            'system',
            'completed'
        )
        
        cursor.execute(insert_query, values)
        
        # Insert detailed records
        if audit_details:
            detail_query = """
                INSERT INTO sync_audit_details 
                (sync_id, record_id, operation_type, old_values, new_values, changes_made, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            detail_values = []
            for detail in audit_details:
                detail_values.append((
                    sync_id,
                    detail['record_id'],
                    detail['operation_type'],
                    json.dumps(detail['old_values']) if detail['old_values'] else None,
                    json.dumps(detail['new_values']) if detail['new_values'] else None,
                    json.dumps(detail['changes_made']) if detail['changes_made'] else None,
                    detail['error_message']
                ))
            
            cursor.executemany(detail_query, detail_values)
        
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Failed to create audit log: {e}")
        # Don't fail the main operation if audit logging fails

def create_error_audit_log(sync_id: str, table_name: str, error_message: str, processing_time: float):
    """Create audit log for failed sync operations"""
    from datetime import datetime
    import json
    
    try:
        ensure_audit_tables()
        
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO sync_audit_log 
            (sync_id, table_name, sync_timestamp, total_source_records, 
             total_target_records, matched_count, unmatched_count, 
             missing_count, extra_count, synced_records, 
             processing_time_seconds, user_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            sync_id,
            table_name,
            datetime.now(),
            0, 0, 0, 0, 0, 0,  # counts
            json.dumps([]),
            processing_time,
            'system',
            f'failed: {error_message}'
        )
        
        cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"Failed to create error audit log: {e}")

def ensure_audit_tables():
    """Ensure audit tables exist in the target database"""
    connection = None
    try:
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor()
        
        # Create main sync audit log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_audit_log (
                sync_id VARCHAR(100) PRIMARY KEY,
                table_name VARCHAR(100) NOT NULL,
                sync_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_source_records INT DEFAULT 0,
                total_target_records INT DEFAULT 0,
                matched_count INT DEFAULT 0,
                unmatched_count INT DEFAULT 0,
                missing_count INT DEFAULT 0,
                extra_count INT DEFAULT 0,
                synced_records JSON,
                processing_time_seconds DECIMAL(10,3) DEFAULT 0,
                user_id VARCHAR(100) DEFAULT 'system',
                status VARCHAR(200) DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create detailed sync audit table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_audit_details (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sync_id VARCHAR(100) NOT NULL,
                record_id VARCHAR(100) NOT NULL,
                operation_type VARCHAR(50) NOT NULL,
                old_values JSON,
                new_values JSON,
                changes_made JSON,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_sync_id (sync_id),
                INDEX idx_record_id (record_id)
            )
        """)
        
        connection.commit()
        cursor.close()
        
    except Exception as e:
        if connection:
            connection.rollback()
        print(f"Failed to ensure audit tables: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
