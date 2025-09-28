#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB

def check_sync_results():
    """Check what happened to the data after sync"""
    
    print("CHECKING SYNC RESULTS:")
    print("=" * 50)
    
    try:
        # Connect to both databases
        source_conn = mysql.connector.connect(**SOURCE_DB)
        target_conn = mysql.connector.connect(**TARGET_DB)
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)
        
        # Check a few synced records
        print("\nCHECKING IDs 1-3 (should be synced):")
        
        print("\nSOURCE DATA:")
        source_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id IN (1,2,3) ORDER BY id")
        source_records = source_cursor.fetchall()
        for r in source_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print("\nTARGET DATA:")
        target_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id IN (1,2,3) ORDER BY id")
        target_records = target_cursor.fetchall()
        for r in target_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        # Check if SCD columns were added
        print("\nSCD COLUMNS ADDED TO TARGET:")
        target_cursor.execute("SHOW COLUMNS FROM customers")
        all_columns = target_cursor.fetchall()
        
        scd_columns = [col for col in all_columns if 'previous' in col['Field'] or col['Field'] in ['record_status', 'last_updated', 'effective_date', 'end_date', 'is_current']]
        
        if scd_columns:
            for col in scd_columns:
                print(f"  {col['Field']} ({col['Type']})")
        else:
            print("  No SCD columns found!")
        
        # Check record count
        target_cursor.execute("SELECT COUNT(*) as count FROM customers")
        target_count = target_cursor.fetchone()['count']
        
        source_cursor.execute("SELECT COUNT(*) as count FROM customers")  
        source_count = source_cursor.fetchone()['count']
        
        print(f"\nRECORD COUNTS:")
        print(f"  Source: {source_count}")
        print(f"  Target: {target_count}")
        
        # Check if records are actually different
        print(f"\nCOMPARING ID 1 IN DETAIL:")
        source_cursor.execute("SELECT * FROM customers WHERE id = 1")
        src_1 = source_cursor.fetchone()
        target_cursor.execute("SELECT * FROM customers WHERE id = 1")  
        tgt_1 = target_cursor.fetchone()
        
        print("Source ID 1:", src_1)
        print("Target ID 1:", tgt_1)
        
        # Check for differences
        if src_1 and tgt_1:
            differences = []
            for key in src_1:
                if key in tgt_1 and src_1[key] != tgt_1[key]:
                    differences.append(f"{key}: '{src_1[key]}' vs '{tgt_1[key]}'")
            
            if differences:
                print("DIFFERENCES FOUND:")
                for diff in differences:
                    print(f"  {diff}")
            else:
                print("NO DIFFERENCES - RECORDS MATCH!")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
    except Exception as e:
        print(f"Error checking sync results: {e}")

if __name__ == "__main__":
    check_sync_results()
