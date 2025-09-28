#!/usr/bin/env python3

import mysql.connector
from config.db_config import TARGET_DB

def check_alex_history():
    conn = mysql.connector.connect(**TARGET_DB)
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT surrogate_id, name, status, amount, is_current, record_status
        FROM customers_scd2 
        WHERE business_id = 100 
        ORDER BY effective_date
    """)
    
    records = cursor.fetchall()
    
    print("ALEX CAREER PROGRESSION (business_id 100):")
    for i, r in enumerate(records, 1):
        current = "CURRENT" if r['is_current'] else "EXPIRED"
        print(f"Row {i}: {r['name']} | {r['status']} | ${r['amount']} | {current}")
    
    print(f"\nTOTAL ROWS FOR ALEX: {len(records)}")
    
    if len(records) >= 3:
        print("SUCCESS: TRUE SCD2 - Multiple historical versions created!")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_alex_history()
