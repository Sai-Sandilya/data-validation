#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB

def show_scd_samples():
    """Show sample SCD test data"""
    
    print("="*70)
    print("SCD TEST DATA SAMPLES")
    print("="*70)
    
    try:
        # Connect to both databases
        source_conn = mysql.connector.connect(**SOURCE_DB)
        target_conn = mysql.connector.connect(**TARGET_DB)
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)
        
        print("\nSCD TYPE 2 TEST SCENARIOS (IDs 1-3):")
        print("-" * 50)
        
        # Show SCD2 samples
        source_cursor.execute("SELECT id, name, email, status, amount FROM customers WHERE id IN (1,2,3) ORDER BY id")
        src_records = source_cursor.fetchall()
        
        target_cursor.execute("SELECT id, name, email, status, amount FROM customers WHERE id IN (1,2,3) ORDER BY id")
        tgt_records = target_cursor.fetchall()
        
        print("SOURCE (NEW values - what we want to sync TO):")
        for r in src_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
            
        print("\nTARGET (OLD values - what currently exists):")
        for r in tgt_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print("\nSCD TYPE 3 TEST SCENARIOS (IDs 11-13):")
        print("-" * 50)
        
        # Show SCD3 samples
        source_cursor.execute("SELECT id, name, email, status, amount FROM customers WHERE id IN (11,12,13) ORDER BY id")
        src_records = source_cursor.fetchall()
        
        target_cursor.execute("SELECT id, name, email, status, amount FROM customers WHERE id IN (11,12,13) ORDER BY id")
        tgt_records = target_cursor.fetchall()
        
        print("SOURCE (NEW values):")
        for r in src_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
            
        print("\nTARGET (OLD values):")
        for r in tgt_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print("\nMISSING RECORDS (IDs 31-33):")
        print("-" * 50)
        source_cursor.execute("SELECT id, name, status FROM customers WHERE id IN (31,32,33) ORDER BY id")
        missing_records = source_cursor.fetchall()
        
        print("SOURCE (exists) -> TARGET (missing):")
        for r in missing_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']}")
            
        print("\nEXTRA RECORDS (IDs 41-42):")
        print("-" * 50)
        target_cursor.execute("SELECT id, name, status FROM customers WHERE id IN (41,42) ORDER BY id")
        extra_records = target_cursor.fetchall()
        
        print("TARGET (exists) -> SOURCE (missing):")
        for r in extra_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']}")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
        print("\n" + "="*70)
        print("TESTING INSTRUCTIONS:")
        print("="*70)
        print("1. Go to http://localhost:5173")
        print("2. In SCD Configuration, try both SCD2 and SCD3")
        print("3. Click eye button to see detailed differences")
        print("4. Select IDs 1-3 and sync with SCD2 (creates new rows)")
        print("5. Select IDs 11-13 and sync with SCD3 (updates in-place)")
        print("6. Use SQL Query tab to see the different results!")
        
    except Exception as e:
        print(f"Error showing samples: {e}")

if __name__ == "__main__":
    show_scd_samples()
