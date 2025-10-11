from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import mysql.connector
from mysql.connector import Error
# Spark imports removed - using MySQL-only mode
from config.db_config import SOURCE_DB, TARGET_DB
from config.settings import AUDIT_COLS, AUDIT_COLUMN_PATTERNS
# Removed imports for deleted modules

router = APIRouter()

class ComparisonRequest(BaseModel):
    table: str
    region: Optional[str] = None

class MultiTableComparisonRequest(BaseModel):
    tables: List[str]
    region: Optional[str] = None
    scd_type: Optional[str] = "scd3"  # "scd2" or "scd3"

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

@router.post("/compare-multiple-tables")
def compare_multiple_tables(request: MultiTableComparisonRequest):
    """Compare multiple tables between source and target databases with lazy evaluation for large datasets"""
    try:
        results = []
        
        for table in request.tables:
            try:
                # Check if we need lazy evaluation (for large datasets)
                source_count = get_table_count(SOURCE_DB, table)
                target_count = get_table_count(TARGET_DB, table)
                
                # Use lazy evaluation for datasets > 50,000,000 records (temporarily increased threshold for testing)
                if source_count > 50_000_000 or target_count > 50_000_000:
                    print(f"Using lazy evaluation for {table} (source: {source_count}, target: {target_count})")
                    result = compare_with_lazy_evaluation(table, request.region)
                else:
                    # Use regular comparison for smaller datasets
                    source_data = get_table_data(SOURCE_DB, table, request.region)
                    target_data = get_table_data(TARGET_DB, table, request.region)
                    
                    # Calculate hash for each record
                    source_with_hash = add_hash_to_records(source_data)
                    target_with_hash = add_hash_to_records(target_data)
                    
                    # Compare records
                    comparison = compare_records(source_with_hash, target_with_hash)
                    
                    result = ComparisonResult(
                        table=table,
                        source_count=len(source_data),
                        target_count=len(target_data),
                        matched_count=comparison['matched'],
                        unmatched_count=comparison['unmatched'],
                        missing_in_target=comparison['missing'],
                        extra_in_target=comparison['extra'],
                        status="Completed"
                    )
                
                results.append(result)
                
            except Exception as table_error:
                # If one table fails, still process others
                error_result = ComparisonResult(
                    table=table,
                    source_count=0,
                    target_count=0,
                    matched_count=0,
                    unmatched_count=0,
                    missing_in_target=0,
                    extra_in_target=0,
                    status=f"Error: {str(table_error)}"
                )
                results.append(error_result)
        
        return {
            "results": results,
            "total_tables": len(request.tables),
            "scd_type": request.scd_type,
            "status": "Completed"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Multi-table comparison failed: {str(e)}")

@router.post("/intelligent-compare")
def intelligent_compare_tables(request: MultiTableComparisonRequest):
    """Intelligent comparison using basic table analysis"""
    try:
        results = []
        
        for table in request.tables:
            try:
                # Get data using basic comparison
                source_data = get_table_data(SOURCE_DB, table, request.region)
                target_data = get_table_data(TARGET_DB, table, request.region)
                
                # Use basic comparison
                source_with_hash = add_hash_to_records(source_data)
                target_with_hash = add_hash_to_records(target_data)
                
                comparison = compare_records(source_with_hash, target_with_hash)
                
                result = ComparisonResult(
                    table=table,
                    source_count=len(source_data),
                    target_count=len(target_data),
                    matched_count=comparison['matched'],
                    unmatched_count=comparison['unmatched'],
                    missing_in_target=comparison['missing'],
                    extra_in_target=comparison['extra'],
                    status="Completed"
                )
                
                results.append(result)
                
            except Exception as table_error:
                error_result = ComparisonResult(
                    table=table,
                    source_count=0,
                    target_count=0,
                    matched_count=0,
                    unmatched_count=0,
                    missing_in_target=0,
                    extra_in_target=0,
                    status=f"Error: {str(table_error)}"
                )
                results.append(error_result)
        
        return {
            "results": results,
            "scd_type": request.scd_type,
            "status": "Completed with Basic Intelligence"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Intelligent comparison failed: {str(e)}")

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

@router.get("/available-tables")
def get_available_tables():
    """Get list of available tables from both source and target databases"""
    try:
        # Use direct MySQL connection for table listing
        source_connection = mysql.connector.connect(
            host=SOURCE_DB["host"],
            port=int(SOURCE_DB["port"]),
            database=SOURCE_DB["database"],
            user=SOURCE_DB["user"],
            password=SOURCE_DB["password"]
        )
        
        target_connection = mysql.connector.connect(
            host=TARGET_DB["host"],
            port=int(TARGET_DB["port"]),
            database=TARGET_DB["database"],
            user=TARGET_DB["user"],
            password=TARGET_DB["password"]
        )
        
        # Get tables from both databases
        source_cursor = source_connection.cursor()
        source_cursor.execute("SHOW TABLES")
        source_tables = [row[0] for row in source_cursor.fetchall()]
        
        target_cursor = target_connection.cursor()
        target_cursor.execute("SHOW TABLES")
        target_tables = [row[0] for row in target_cursor.fetchall()]
        
        # Find common tables
        common_tables = list(set(source_tables) & set(target_tables))
        source_only = list(set(source_tables) - set(target_tables))
        target_only = list(set(target_tables) - set(source_tables))
        
        # Close connections
        source_cursor.close()
        target_cursor.close()
        source_connection.close()
        target_connection.close()
        
        return {
            "source_tables": source_tables,
            "target_tables": target_tables,
            "common_tables": common_tables,
            "source_only": source_only,
            "target_only": target_only,
            "total_source": len(source_tables),
            "total_target": len(target_tables),
            "total_common": len(common_tables),
            "source_database_type": "mysql",
            "target_database_type": "mysql"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get tables: {str(e)}")

@router.post("/detailed-comparison")
def get_detailed_comparison(request: ComparisonRequest):
    """Get detailed record-by-record comparison"""
    try:
        source_data = get_table_data(SOURCE_DB, request.table, request.region)
        target_data = get_table_data(TARGET_DB, request.table, request.region)
        
        # Removed debug logging
        
        source_with_hash = add_hash_to_records(source_data)
        target_with_hash = add_hash_to_records(target_data)
        
        # Handle SCD2 tables specially (compare by original primary key)
        # Check if this is an SCD2 sync by looking for SCD2 columns in target
        is_scd2_sync = False
        if target_with_hash and len(target_with_hash) > 0:
            target_record = target_with_hash[0]
            # Check if target has SCD2 columns
            scd2_columns = ['effective_date', 'end_date', 'is_current', 'record_status']
            has_scd2_columns = any(col in target_record for col in scd2_columns)
            is_scd2_sync = has_scd2_columns
        
        if is_scd2_sync:
            detailed_records = get_scd2_detailed_differences(source_with_hash, target_with_hash)
        else:
            detailed_records = get_detailed_differences(source_with_hash, target_with_hash)
        
        return {
            "table": request.table,
            "total_differences": len(detailed_records),
            "records": detailed_records  # Return all records for accurate sync
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Detailed comparison failed: {str(e)}")

def get_table_count(db_config: dict, table: str) -> int:
    """Get count of records in table for lazy evaluation decision"""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=db_config["host"],
            port=int(db_config["port"]),
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            connection_timeout=300,
        )
        
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.close()
        
        return count
        
    except Error as e:
        print(f"Error getting count for {table}: {e}")
        return 0
    finally:
        if connection and connection.is_connected():
            connection.close()

def compare_with_lazy_evaluation(table: str, region: Optional[str] = None):
    """Compare table using lazy evaluation for large datasets"""
    try:
        from services.lazy_sync_service import LazySyncService
        
        # Initialize lazy sync service
        lazy_service = LazySyncService()
        
        # Use lazy evaluation for comparison
        result = lazy_service.lazy_compare_tables(
            source_config=SOURCE_DB,
            target_config=TARGET_DB,
            table_name=table,
            business_keys=['id']  # Assume 'id' is the business key
        )
        
        return ComparisonResult(
            table=table,
            source_count=result.get('source_count', 0),
            target_count=result.get('target_count', 0),
            matched_count=result.get('matched_count', 0),
            unmatched_count=result.get('unmatched_count', 0),
            missing_in_target=result.get('missing_count', 0),
            extra_in_target=result.get('extra_count', 0),
            status="Completed with Lazy Evaluation"
        )
        
    except Exception as e:
        print(f"Lazy evaluation failed for {table}: {e}")
        # Fallback to regular comparison
        source_data = get_table_data(SOURCE_DB, table, region)
        target_data = get_table_data(TARGET_DB, table, region)
        
        source_with_hash = add_hash_to_records(source_data)
        target_with_hash = add_hash_to_records(target_data)
        
        comparison = compare_records(source_with_hash, target_with_hash)
        
        return ComparisonResult(
            table=table,
            source_count=len(source_data),
            target_count=len(target_data),
            matched_count=comparison['matched'],
            unmatched_count=comparison['unmatched'],
            missing_in_target=comparison['missing'],
            extra_in_target=comparison['extra'],
            status="Completed (Fallback)"
        )

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
            connection_timeout=300,  # Increased to 5 minutes for large datasets
        )
        
        cursor = connection.cursor(dictionary=True)
        
        # Set query timeout for large datasets
        cursor.execute("SET SESSION wait_timeout = 300")
        cursor.execute("SET SESSION interactive_timeout = 300")
        
        # Build query
        query = f"SELECT * FROM {table}"
        params = []
        
        # Only add region filter if region is provided and not "ALL"
        if region and region != "ALL":
            # Check if region column exists
            cursor.execute(f"SHOW COLUMNS FROM {table} LIKE 'region'")
            if cursor.fetchone():
                query += " WHERE region = %s"
                params.append(region)
            
        # Remove limit for accurate comparison - use lazy evaluation for large datasets
        # NO LIMIT - Process all records
        
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
    """Compare SCD2 records by original primary key (only current target records)"""
    if not source_records and not target_records:
        return {'matched': 0, 'unmatched': 0, 'missing': 0, 'extra': 0}
    
    # Get the original primary key column (first column, usually 'id')
    pk_column = list(source_records[0].keys())[0] if source_records else list(target_records[0].keys())[0]
    
    # For SCD2, create lookups by original primary key (business key)
    source_lookup = {record[pk_column]: record for record in source_records}
    
    # For target, only get current records (is_current = True/1)
    current_target_records = [
        record for record in target_records 
        if record.get('is_current', False) in [True, 1, '1']
    ]
    target_lookup = {record[pk_column]: record for record in current_target_records}
    
    # Get all extra (non-current) records in target for counting
    all_target_business_ids = set(record[pk_column] for record in target_records)
    current_target_business_ids = set(record[pk_column] for record in current_target_records)
    
    matched = 0
    unmatched = 0
    missing = 0
    extra = 0
    
    # Check source records against current target records
    for business_key, source_record in source_lookup.items():
        if business_key in target_lookup:
            target_record = target_lookup[business_key]
            # Compare hashes (excluding SCD2 system columns)
            if source_record['row_hash'] == target_record['row_hash']:
                matched += 1
            else:
                unmatched += 1
        else:
            missing += 1
    
    # Check for extra business entities in target (that don't exist in source)
    for business_key in all_target_business_ids:
        if business_key not in source_lookup:
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
    """Get detailed differences for SCD2 tables (compare by original primary key)"""
    if not source_records and not target_records:
        return []
    
    # Get the original primary key column (first column, usually 'id')
    pk_column = list(source_records[0].keys())[0] if source_records else list(target_records[0].keys())[0]
    
    # For SCD2, compare by original primary key (business key)
    source_lookup = {record[pk_column]: record for record in source_records}
    
    # For target, only compare against current records (is_current = True/1)
    current_target_records = [
        record for record in target_records 
        if record.get('is_current', False) in [True, 1, '1']
    ]
    target_lookup = {record[pk_column]: record for record in current_target_records}
    
    # Get all extra business IDs in target (that don't exist in source)
    all_target_business_ids = set(record[pk_column] for record in target_records)
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
            (record for record in current_target_records if record[pk_column] == business_id), 
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
    """Sync selected records from source to target database with simple logic"""
    import time
    from datetime import datetime
    
    start_time = time.time()
    sync_id = f"SYNC_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    
    print(f"🔍 DEBUG: Simple sync started - ID: {sync_id}")
    print(f"🔍 DEBUG: Request table: {request.table}")
    print(f"🔍 DEBUG: Selected records count: {len(request.selected_records)}")
    print(f"🔍 DEBUG: Selected records (first 10): {request.selected_records[:10]}")
    
    try:
        # Get source and target data for comparison
        print(f"🔍 DEBUG: Getting source data...")
        source_data = get_table_data(SOURCE_DB, request.table)
        print(f"🔍 DEBUG: Source data count: {len(source_data)}")
        
        print(f"🔍 DEBUG: Getting target data...")
        target_data = get_table_data(TARGET_DB, request.table)
        print(f"🔍 DEBUG: Target data count: {len(target_data)}")
        
        # Filter selected records
        print(f"🔍 DEBUG: Filtering selected records...")
        pk_column = get_primary_key_column(source_data)
        print(f"🔍 DEBUG: Primary key column: {pk_column}")
        
        selected_source_records = [
            record for record in source_data 
            if record[pk_column] in request.selected_records
        ]
        
        print(f"🔍 DEBUG: Selected source records count: {len(selected_source_records)}")
        print(f"🔍 DEBUG: Selected source records (first 5): {[r[pk_column] for r in selected_source_records[:5]]}")
        
        if not selected_source_records:
            print(f"🔍 DEBUG: No records found to sync!")
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
        
        # Perform the actual sync with simple logic
        print(f"🔍 DEBUG: Starting simple sync operation...")
        print(f"🔍 DEBUG: Table: {request.table}")
        print(f"🔍 DEBUG: Records to sync: {len(selected_source_records)}")
        
        synced_count = insert_records_to_target(request.table, selected_source_records)
        
        print(f"🔍 DEBUG: Sync completed - synced count: {synced_count}")
        
        # Calculate processing time
        processing_time = time.time() - start_time
        print(f"🔍 DEBUG: Processing time: {processing_time:.2f} seconds")
        
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

def insert_records_to_target(table: str, records: List[Dict]):
    """Insert/Update records into target database with simple logic"""
    print(f"🔍 DEBUG: insert_records_to_target called")
    print(f"🔍 DEBUG: Table: {table}")
    print(f"🔍 DEBUG: Records count: {len(records)}")
    
    if not records:
        print(f"🔍 DEBUG: No records to insert")
        return 0
    
    connection = None
    try:
        print(f"🔍 DEBUG: Connecting to target database...")
        connection = mysql.connector.connect(
            host=TARGET_DB["host"],
            port=int(TARGET_DB["port"]),
            database=TARGET_DB["database"],
            user=TARGET_DB["user"],
            password=TARGET_DB["password"],
            connection_timeout=300,  # Increased to 5 minutes for large datasets
        )
        print(f"🔍 DEBUG: Database connection successful")
        
        cursor = connection.cursor(dictionary=True)
        
        # Set query timeout for large datasets
        cursor.execute("SET SESSION wait_timeout = 300")
        cursor.execute("SET SESSION interactive_timeout = 300")
        
        # Simple sync - no SCD logic needed
        print(f"🔍 DEBUG: Starting simple sync for {len(records)} records")
        
        synced_count = 0
        pk_column = get_primary_key_column(records)
        print(f"🔍 DEBUG: Primary key column: {pk_column}")
        print(f"🔍 DEBUG: Processing {len(records)} records...")
        
        for i, record in enumerate(records):
            if i < 5:  # Debug first 5 records
                print(f"🔍 DEBUG: Processing record {i+1}: {record.get(pk_column, 'NO_ID')}")
            
            # Remove the row_hash if it exists (it's calculated, not stored)
            record_copy = {k: v for k, v in record.items() if k != 'row_hash'}
            pk_value = record_copy[pk_column]
            
            # Simple sync logic - check if record exists
            cursor.execute(f"SELECT * FROM {table} WHERE {pk_column} = %s", (pk_value,))
            existing_record = cursor.fetchone()
            
            if existing_record:
                if i < 5:  # Debug first 5 records
                    print(f"🔍 DEBUG: Record {pk_value} exists - updating")
                # Update existing record
                result = perform_simple_update(cursor, table, record_copy, pk_column, pk_value)
                synced_count += result
                if i < 5:  # Debug first 5 records
                    print(f"🔍 DEBUG: Update result: {result}")
            else:
                if i < 5:  # Debug first 5 records
                    print(f"🔍 DEBUG: Record {pk_value} missing - inserting")
                # Insert new record
                result = perform_simple_insert(cursor, table, record_copy)
                synced_count += result
                if i < 5:  # Debug first 5 records
                    print(f"🔍 DEBUG: Insert result: {result}")
        
        connection.commit()
        cursor.close()
        
        print(f"🔍 DEBUG: Final synced count: {synced_count}")
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
    """Ensure SCD Type 2 columns exist by creating a separate SCD2 table"""
    print(f"🔍 DEBUG: Ensuring SCD2 columns for table {table}")
    
    # Create SCD2 table name
    scd2_table = f"{table}_scd2"
    
    # Check if SCD2 table already exists
    cursor.execute(f"SHOW TABLES LIKE '{scd2_table}'")
    if cursor.fetchone():
        print(f"🔍 DEBUG: SCD2 table {scd2_table} already exists")
        return scd2_table
    
    print(f"🔍 DEBUG: Creating SCD2 table {scd2_table}")
    
    # Create SCD2 table with proper columns
    if records:
        first_record = records[0]
        columns_def = []
        
        for col, value in first_record.items():
            if col == 'id':
                columns_def.append(f"{col} INT")
            elif isinstance(value, int):
                columns_def.append(f"{col} INT")
            elif isinstance(value, str):
                columns_def.append(f"{col} VARCHAR(255)")
            else:
                columns_def.append(f"{col} TEXT")
        
        # Add SCD2 system columns
        columns_def.extend([
            "surrogate_id INT PRIMARY KEY AUTO_INCREMENT",
            "effective_date DATETIME DEFAULT CURRENT_TIMESTAMP",
            "end_date DATETIME DEFAULT '9999-12-31 23:59:59'",
            "is_current BOOLEAN DEFAULT TRUE",
            "record_status VARCHAR(20) DEFAULT 'CURRENT'"
        ])
        
        create_query = f"CREATE TABLE {scd2_table} ({', '.join(columns_def)})"
        cursor.execute(create_query)
        print(f"🔍 DEBUG: Created SCD2 table {scd2_table}")
        
        # Copy existing data to SCD2 table
        cursor.execute(f"INSERT INTO {scd2_table} SELECT *, ROW_NUMBER() OVER (ORDER BY id) + 1000000, NOW(), '9999-12-31 23:59:59', TRUE, 'CURRENT' FROM {table}")
        print(f"🔍 DEBUG: Copied existing data to SCD2 table")
    
    print(f"🔍 DEBUG: SCD2 table {scd2_table} ready")
    return scd2_table

def perform_simple_insert(cursor, table: str, record: Dict):
    """Perform simple insert - insert new record"""
    # Build insert query
    columns = list(record.keys())
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join(columns)
    
    insert_query = f"""
        INSERT INTO {table} ({column_names})
        VALUES ({placeholders})
    """
    
    values = [record[col] for col in columns]
    cursor.execute(insert_query, values)
    
    print(f"🔍 DEBUG: Simple insert successful for record {record.get('id', 'unknown')}")
    return 1

def perform_simple_update(cursor, table: str, record: Dict, pk_column: str, pk_value):
    """Perform simple update - update existing record"""
    # Build update query
    set_clauses = []
    values = []
    
    for col, value in record.items():
        if col != pk_column:  # Don't update primary key
            set_clauses.append(f"{col} = %s")
            values.append(value)
    
    values.append(pk_value)  # For WHERE clause
    
    update_query = f"""
        UPDATE {table} 
        SET {', '.join(set_clauses)}
        WHERE {pk_column} = %s
    """
    
    cursor.execute(update_query, values)
    
    print(f"🔍 DEBUG: Simple update successful for record {pk_value}")
    return cursor.rowcount

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

# Removed intelligent comparison functions that used deleted modules

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
