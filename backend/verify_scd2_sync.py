#!/usr/bin/env python3

import mysql.connector
from config.db_config import TARGET_DB

def verify_scd2_sync():
    """Verify SCD2 sync created proper historical tracking"""
    
    print("=== VERIFYING SCD2 SYNC RESULTS ===")
    print("Checking Alex's career progression (business_id 100)...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor(dictionary=True)
        
        # Get Alex's complete career history
        cursor.execute("""
            SELECT surrogate_id, business_id, name, status, amount, 
                   effective_date, end_date, is_current, record_status
            FROM customers_scd2 
            WHERE business_id = 100 
            ORDER BY effective_date
        """)
        
        alex_records = cursor.fetchall()
        
        print(f"\nFound {len(alex_records)} records for Alex (business_id 100):")
        print("-" * 80)
        
        for i, record in enumerate(alex_records, 1):
            current_status = "✅ CURRENT" if record['is_current'] else "❌ EXPIRED"
            print(f"Row {i}: {record['name']}")
            print(f"       Status: {record['status']} | Amount: ${record['amount']}")
            print(f"       Effective: {record['effective_date']} → {record['end_date']}")
            print(f"       {current_status} | Record Status: {record['record_status']}")
            print()
        
        # Check overall SCD2 table state
        cursor.execute("SELECT COUNT(*) as total FROM customers_scd2")
        total_count = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as current FROM customers_scd2 WHERE is_current = TRUE")
        current_count = cursor.fetchone()['current']
        
        cursor.execute("SELECT COUNT(*) as expired FROM customers_scd2 WHERE is_current = FALSE")
        expired_count = cursor.fetchone()['expired']
        
        print("=== SCD2 TABLE SUMMARY ===")
        print(f"Total records: {total_count}")
        print(f"Current records: {current_count}")
        print(f"Expired/Historical records: {expired_count}")
        
        # Verify TRUE SCD2 behavior
        if len(alex_records) > 1:
            print("\n🎯 SUCCESS: TRUE SCD2 - Multiple rows created for Alex!")
            print("✅ Historical tracking is working correctly")
        else:
            print("\n⚠️  WARNING: Only 1 row found - might not be true SCD2")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_scd2_sync()
