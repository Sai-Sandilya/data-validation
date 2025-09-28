from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import mysql.connector
from mysql.connector import Error
from config.db_config import TARGET_DB
from datetime import datetime
import json
import csv
import io
import base64

router = APIRouter()

class SyncLogEntry(BaseModel):
    sync_id: str
    table_name: str
    sync_timestamp: datetime
    total_source_records: int
    total_target_records: int
    matched_count: int
    unmatched_count: int
    missing_count: int
    extra_count: int
    synced_records: List[Any]
    processing_time_seconds: float
    user_id: Optional[str] = "system"
    status: str = "completed"

class DetailedSyncLog(BaseModel):
    sync_id: str
    record_id: Any
    operation_type: str  # 'matched', 'unmatched_scd3', 'missing_insert', 'error'
    old_values: Optional[Dict[str, Any]]
    new_values: Optional[Dict[str, Any]]
    changes_made: List[str]
    error_message: Optional[str] = None

@router.post("/create-sync-log")
def create_sync_log(log_entry: SyncLogEntry):
    """Create a comprehensive sync log entry"""
    try:
        # Generate unique sync ID if not provided
        if not log_entry.sync_id:
            log_entry.sync_id = f"SYNC_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Ensure audit tables exist
        ensure_audit_tables()
        
        # Insert main sync log
        insert_sync_summary(log_entry)
        
        # Insert detailed records if provided
        if hasattr(log_entry, 'detailed_records'):
            insert_detailed_records(log_entry.sync_id, log_entry.detailed_records)
        
        return {
            "success": True,
            "sync_id": log_entry.sync_id,
            "message": "Sync log created successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create sync log: {str(e)}")

@router.post("/create-validation-audit")
def create_validation_audit(log_data: Dict[str, Any]):
    """Create a comprehensive validation audit log with detailed records"""
    try:
        print(f"[DEBUG] Creating validation audit with ID: {log_data.get('sync_id')}")
        
        # Ensure audit tables exist
        ensure_audit_tables()
        
        # Create main log entry
        log_entry = SyncLogEntry(
            sync_id=log_data['sync_id'],
            table_name=log_data['table_name'],
            sync_timestamp=datetime.fromisoformat(log_data['sync_timestamp'].replace('Z', '+00:00')),
            total_source_records=log_data['total_source_records'],
            total_target_records=log_data['total_target_records'],
            matched_count=log_data['matched_count'],
            unmatched_count=log_data['unmatched_count'],
            missing_count=log_data['missing_count'],
            extra_count=log_data['extra_count'],
            synced_records=log_data['synced_records'],
            processing_time_seconds=log_data['processing_time_seconds'],
            user_id=log_data['user_id'],
            status=log_data['status']
        )
        
        # Insert main sync log
        insert_sync_summary(log_entry)
        
        # Insert detailed records if provided
        if 'detailed_records' in log_data and log_data['detailed_records']:
            detailed_records = []
            for record in log_data['detailed_records']:
                detailed_records.append(DetailedSyncLog(
                    sync_id=record['sync_id'],
                    record_id=record['record_id'],
                    operation_type=record['operation_type'],
                    old_values=record.get('old_values'),
                    new_values=record.get('new_values'),
                    changes_made=record.get('changes_made', []),
                    error_message=record.get('error_message')
                ))
            
            insert_detailed_records(log_data['sync_id'], detailed_records)
            print(f"[DEBUG] Inserted {len(detailed_records)} detailed audit records")
        
        return {
            "success": True,
            "sync_id": log_data['sync_id'],
            "message": "Validation audit created successfully with detailed records"
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to create validation audit: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to create validation audit: {str(e)}")

@router.get("/sync-logs")
def get_sync_logs(limit: int = 50, table_name: Optional[str] = None):
    """Get list of sync logs with optional filtering"""
    try:
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor(dictionary=True)
        
        query = """
            SELECT sync_id, table_name, sync_timestamp, total_source_records, 
                   total_target_records, matched_count, unmatched_count, 
                   missing_count, processing_time_seconds, status
            FROM sync_audit_log 
        """
        params = []
        
        if table_name:
            query += " WHERE table_name = %s"
            params.append(table_name)
        
        query += " ORDER BY sync_timestamp DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        logs = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return {"logs": logs}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve sync logs: {str(e)}")

@router.get("/sync-log-details/{sync_id}")
def get_sync_log_details(sync_id: str):
    """Get detailed sync log for a specific sync operation"""
    try:
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor(dictionary=True)
        
        # Get summary
        cursor.execute("SELECT * FROM sync_audit_log WHERE sync_id = %s", (sync_id,))
        summary = cursor.fetchone()
        
        if not summary:
            raise HTTPException(status_code=404, detail="Sync log not found")
        
        # Get detailed records
        cursor.execute("""
            SELECT record_id, operation_type, old_values, new_values, 
                   changes_made, error_message
            FROM sync_audit_details 
            WHERE sync_id = %s 
            ORDER BY record_id
        """, (sync_id,))
        details = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Parse JSON fields
        for detail in details:
            if detail['old_values']:
                detail['old_values'] = json.loads(detail['old_values'])
            if detail['new_values']:
                detail['new_values'] = json.loads(detail['new_values'])
            if detail['changes_made']:
                detail['changes_made'] = json.loads(detail['changes_made'])
        
        return {
            "summary": summary,
            "details": details,
            "total_detail_records": len(details)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve sync log details: {str(e)}")

@router.get("/download-sync-report/{sync_id}")
def download_sync_report(sync_id: str, format: str = "csv"):
    """Download sync report in various formats"""
    try:
        print(f"[DEBUG] Download request - Sync ID: {sync_id}, Format: {format}")
        
        # Validate format first
        if format.lower() not in ["csv", "json"]:
            raise HTTPException(status_code=400, detail="Unsupported format. Use 'csv' or 'json'")
        
        # Get sync log details
        log_data = get_sync_log_details(sync_id)
        print(f"[DEBUG] Retrieved log data for sync ID: {sync_id}")
        
        if format.lower() == "csv":
            result = generate_csv_report(log_data)
        else:  # json
            result = generate_json_report(log_data)
            
        print(f"[DEBUG] Generated {format} report successfully")
        return result
        
    except HTTPException as he:
        # Re-raise HTTP exceptions as-is
        print(f"[ERROR] HTTP Exception in download: {he.detail}")
        raise he
    except Exception as e:
        print(f"[ERROR] Unexpected error in download: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to generate report: {str(e)}")

def ensure_audit_tables():
    """Ensure audit tables exist in the database"""
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
                status VARCHAR(50) DEFAULT 'completed',
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
                FOREIGN KEY (sync_id) REFERENCES sync_audit_log(sync_id) ON DELETE CASCADE,
                INDEX idx_sync_id (sync_id),
                INDEX idx_record_id (record_id)
            )
        """)
        
        connection.commit()
        cursor.close()
        
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection and connection.is_connected():
            connection.close()

def insert_sync_summary(log_entry: SyncLogEntry):
    """Insert sync summary into audit log"""
    connection = None
    try:
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
            log_entry.sync_id,
            log_entry.table_name,
            log_entry.sync_timestamp,
            log_entry.total_source_records,
            log_entry.total_target_records,
            log_entry.matched_count,
            log_entry.unmatched_count,
            log_entry.missing_count,
            log_entry.extra_count,
            json.dumps(log_entry.synced_records),
            log_entry.processing_time_seconds,
            log_entry.user_id,
            log_entry.status
        )
        
        cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()
        
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection and connection.is_connected():
            connection.close()

def insert_detailed_records(sync_id: str, detailed_records: List[DetailedSyncLog]):
    """Insert detailed sync records"""
    if not detailed_records:
        return
        
    connection = None
    try:
        connection = mysql.connector.connect(**TARGET_DB)
        cursor = connection.cursor()
        
        insert_query = """
            INSERT INTO sync_audit_details 
            (sync_id, record_id, operation_type, old_values, new_values, changes_made, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        values_list = []
        for record in detailed_records:
            values = (
                sync_id,
                record.record_id,
                record.operation_type,
                json.dumps(record.old_values) if record.old_values else None,
                json.dumps(record.new_values) if record.new_values else None,
                json.dumps(record.changes_made) if record.changes_made else None,
                record.error_message
            )
            values_list.append(values)
        
        cursor.executemany(insert_query, values_list)
        connection.commit()
        cursor.close()
        
    except Exception as e:
        if connection:
            connection.rollback()
        raise e
    finally:
        if connection and connection.is_connected():
            connection.close()

def generate_csv_report(log_data: Dict):
    """Generate CSV report from sync log data"""
    output = io.StringIO()
    
    # Write summary
    output.write("SYNC SUMMARY\n")
    output.write("=" * 50 + "\n")
    summary = log_data['summary']
    output.write(f"Sync ID: {summary['sync_id']}\n")
    output.write(f"Table: {summary['table_name']}\n")
    output.write(f"Timestamp: {summary['sync_timestamp']}\n")
    output.write(f"Source Records: {summary['total_source_records']}\n")
    output.write(f"Target Records: {summary['total_target_records']}\n")
    output.write(f"Matched: {summary['matched_count']}\n")
    output.write(f"Unmatched: {summary['unmatched_count']}\n")
    output.write(f"Missing: {summary['missing_count']}\n")
    output.write(f"Processing Time: {summary['processing_time_seconds']} seconds\n")
    output.write("\n")
    
    # Write detailed records
    output.write("DETAILED RECORDS\n")
    output.write("=" * 50 + "\n")
    
    writer = csv.writer(output)
    writer.writerow(['Record ID', 'Operation Type', 'Changes Made', 'Error Message'])
    
    for detail in log_data['details']:
        changes = ', '.join(detail['changes_made']) if detail['changes_made'] else ''
        writer.writerow([
            detail['record_id'],
            detail['operation_type'],
            changes,
            detail['error_message'] or ''
        ])
    
    csv_content = output.getvalue()
    output.close()
    
    # Encode as base64 for download
    csv_bytes = csv_content.encode('utf-8')
    csv_base64 = base64.b64encode(csv_bytes).decode('utf-8')
    
    return {
        "filename": f"sync_report_{summary['sync_id']}.csv",
        "content": csv_base64,
        "content_type": "text/csv"
    }

def generate_json_report(log_data: Dict):
    """Generate JSON report from sync log data"""
    json_content = json.dumps(log_data, indent=2, default=str)
    json_bytes = json_content.encode('utf-8')
    json_base64 = base64.b64encode(json_bytes).decode('utf-8')
    
    return {
        "filename": f"sync_report_{log_data['summary']['sync_id']}.json",
        "content": json_base64,
        "content_type": "application/json"
    }
