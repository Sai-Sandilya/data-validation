#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB

def verify_comprehensive_data():
    """Verify comprehensive SCD test data was created"""
    
    print("="*70)
    print("COMPREHENSIVE SCD TEST DATA VERIFICATION")
    print("="*70)
    
    try:
        # Connect to both databases
        source_conn = mysql.connector.connect(**SOURCE_DB)
        target_conn = mysql.connector.connect(**TARGET_DB)
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)
        
        # Check total counts
        source_cursor.execute("SELECT COUNT(*) as count FROM customers")
        source_count = source_cursor.fetchone()['count']
        
        target_cursor.execute("SELECT COUNT(*) as count FROM customers")
        target_count = target_cursor.fetchone()['count']
        
        print(f"TOTAL DATA:")
        print(f"  Source: {source_count} customers")
        print(f"  Target: {target_count} customers")
        
        print(f"\nSCD TYPE 2 TEST SCENARIOS:")
        print("-" * 40)
        
        print(f"CAREER PROGRESSION (IDs 50-54) - Source (NEW):")
        source_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 50 AND 54 ORDER BY id")
        career_records = source_cursor.fetchall()
        for r in career_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nCAREER PROGRESSION (IDs 50-54) - Target (OLD):")
        target_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 50 AND 54 ORDER BY id")
        old_career = target_cursor.fetchall()
        for r in old_career:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nSALARY ADJUSTMENTS (IDs 80-84) - Source (NEW):")
        source_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 80 AND 84 ORDER BY id")
        salary_records = source_cursor.fetchall()
        for r in salary_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nSALARY ADJUSTMENTS (IDs 80-84) - Target (OLD):")
        target_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 80 AND 84 ORDER BY id")
        old_salary = target_cursor.fetchall()
        for r in old_salary:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nSCD TYPE 3 TEST SCENARIOS:")
        print("-" * 40)
        
        print(f"STATUS UPGRADES (IDs 60-64) - Source (NEW):")
        source_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 60 AND 64 ORDER BY id")
        status_records = source_cursor.fetchall()
        for r in status_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nSTATUS UPGRADES (IDs 60-64) - Target (OLD):")
        target_cursor.execute("SELECT id, name, status, amount FROM customers WHERE id BETWEEN 60 AND 64 ORDER BY id")
        old_status = target_cursor.fetchall()
        for r in old_status:
            print(f"  ID {r['id']}: {r['name']} | {r['status']} | ${r['amount']}")
        
        print(f"\nMISSING RECORDS (IDs 90-94) - Source only:")
        source_cursor.execute("SELECT id, name, status FROM customers WHERE id BETWEEN 90 AND 94 ORDER BY id")
        missing_records = source_cursor.fetchall()
        for r in missing_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']}")
        
        print(f"\nEXTRA RECORDS (IDs 95-97) - Target only:")
        target_cursor.execute("SELECT id, name, status FROM customers WHERE id BETWEEN 95 AND 97 ORDER BY id")
        extra_records = target_cursor.fetchall()
        for r in extra_records:
            print(f"  ID {r['id']}: {r['name']} | {r['status']}")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
        print(f"\n" + "="*70)
        print("TESTING INSTRUCTIONS:")
        print("="*70)
        print("1. Go to http://localhost:5173")
        print("2. Configure SCD Type 2, select IDs 50-54 or 80-84, sync")
        print("   -> Should create new rows + expire old ones")
        print("3. Configure SCD Type 3, select IDs 60-64, sync")  
        print("   -> Should update in-place + store previous values")
        print("4. Use SQL Query tab to see different structures")
        print("5. Compare results between SCD2 and SCD3!")
        
    except Exception as e:
        print(f"Error verifying data: {e}")

if __name__ == "__main__":
    verify_comprehensive_data()
