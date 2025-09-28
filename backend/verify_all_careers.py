#!/usr/bin/env python3

import mysql.connector
from config.db_config import TARGET_DB

def verify_all_careers():
    conn = mysql.connector.connect(**TARGET_DB)
    cursor = conn.cursor(dictionary=True)
    
    print("=== SCD2 CAREER PROGRESSIONS ===")
    
    # Check specific people
    people = [100, 101, 102]  # Alex, Maria, David
    
    for person_id in people:
        cursor.execute("""
            SELECT name, status, amount, is_current, record_status
            FROM customers_scd2 
            WHERE business_id = %s 
            ORDER BY effective_date
        """, (person_id,))
        
        records = cursor.fetchall()
        if records:
            name = records[0]['name'].split(' ')[0]  # Get first name
            print(f"\n{name.upper()} (business_id {person_id}):")
            
            for i, r in enumerate(records, 1):
                current = "CURRENT" if r['is_current'] else "EXPIRED"
                print(f"  Row {i}: {r['name']} | {r['status']} | ${r['amount']} | {current}")
            
            print(f"  Total career steps: {len(records)}")
    
    # Overall summary
    cursor.execute("SELECT COUNT(*) as total FROM customers_scd2")
    total = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as current FROM customers_scd2 WHERE is_current = TRUE")
    current = cursor.fetchone()['current']
    
    print(f"\n=== SUMMARY ===")
    print(f"Total records: {total}")
    print(f"Current records: {current}")
    print(f"Historical records: {total - current}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    verify_all_careers()
