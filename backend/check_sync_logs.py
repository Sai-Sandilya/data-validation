#!/usr/bin/env python3

import mysql.connector
from config.db_config import TARGET_DB

def check_sync_logs():
    """Check what sync logs exist in the database"""
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor(dictionary=True)
        
        # Get total count
        cursor.execute('SELECT COUNT(*) as total FROM sync_audit_log')
        total = cursor.fetchone()['total']
        print(f"Total sync logs: {total}")
        
        if total > 0:
            # Get recent logs
            cursor.execute('''
                SELECT sync_id, table_name, sync_timestamp, 
                       matched_count, unmatched_count, missing_count, extra_count
                FROM sync_audit_log 
                ORDER BY sync_timestamp DESC 
                LIMIT 10
            ''')
            logs = cursor.fetchall()
            
            print("\nRecent Sync Logs:")
            for i, log in enumerate(logs, 1):
                print(f"{i}. ID: {log['sync_id']}")
                print(f"   Table: {log['table_name']}")
                print(f"   Time: {log['sync_timestamp']}")
                print(f"   Stats: Matched={log['matched_count']}, Unmatched={log['unmatched_count']}, Missing={log['missing_count']}, Extra={log['extra_count']}")
                print()
        else:
            print("No sync logs found in database!")
            
        conn.close()
        
    except Exception as e:
        print(f"Error checking sync logs: {e}")

def check_audit_details():
    """Check if audit details exist"""
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM sync_audit_details')
        details_count = cursor.fetchone()[0]
        print(f"Total audit detail records: {details_count}")
        
        if details_count > 0:
            cursor.execute('''
                SELECT sync_id, COUNT(*) as detail_count 
                FROM sync_audit_details 
                GROUP BY sync_id 
                ORDER BY sync_id DESC 
                LIMIT 5
            ''')
            details = cursor.fetchall()
            print("\nAudit Details by Sync ID:")
            for detail in details:
                print(f"  {detail[0]}: {detail[1]} detail records")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking audit details: {e}")

if __name__ == "__main__":
    check_sync_logs()
    check_audit_details()
