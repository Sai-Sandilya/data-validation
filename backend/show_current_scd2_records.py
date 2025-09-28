#!/usr/bin/env python3

import mysql.connector
from config.db_config import TARGET_DB

def show_current_scd2_records():
    """Show current SCD2 records for each business entity"""
    
    print("="*80)
    print("SCD TYPE 2 - CURRENT RECORDS IDENTIFICATION")
    print("="*80)
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor(dictionary=True)
        
        # Method 1: Using is_current flag
        print("\nMETHOD 1: Using is_current = TRUE")
        print("-" * 50)
        cursor.execute("""
            SELECT business_id, name, status, amount, 
                   effective_date, end_date, is_current, record_status
            FROM customers_scd2 
            WHERE is_current = TRUE 
            ORDER BY business_id
        """)
        
        current_records = cursor.fetchall()
        for record in current_records:
            print(f"ID {record['business_id']}: {record['name']} | {record['status']} | ${record['amount']} | Current: {record['is_current']}")
        
        print(f"\nTotal CURRENT records: {len(current_records)}")
        
        # Method 2: Using end_date = '9999-12-31 23:59:59'
        print(f"\n\nMETHOD 2: Using end_date = '9999-12-31 23:59:59'")
        print("-" * 50)
        cursor.execute("""
            SELECT business_id, name, status, amount, end_date
            FROM customers_scd2 
            WHERE end_date = '9999-12-31 23:59:59'
            ORDER BY business_id
        """)
        
        active_records = cursor.fetchall()
        for record in active_records:
            print(f"ID {record['business_id']}: {record['name']} | End Date: {record['end_date']}")
        
        print(f"\nTotal ACTIVE records: {len(active_records)}")
        
        # Show Emma's complete timeline
        print(f"\n\nEMMA'S COMPLETE CAREER TIMELINE (business_id 200):")
        print("-" * 60)
        cursor.execute("""
            SELECT surrogate_id, name, status, amount, 
                   effective_date, end_date, is_current, record_status
            FROM customers_scd2 
            WHERE business_id = 200 
            ORDER BY effective_date
        """)
        
        emma_timeline = cursor.fetchall()
        for i, record in enumerate(emma_timeline, 1):
            current_flag = "CURRENT" if record['is_current'] else "EXPIRED"
            print(f"Step {i}: {record['name']}")
            print(f"        Status: {record['status']} | Salary: ${record['amount']}")
            print(f"        Period: {record['effective_date']} -> {record['end_date']}")
            print(f"        Flag: {current_flag} | Record: {record['record_status']}")
            print()
        
        # Show business entities with multiple versions
        print(f"\nBUSINESS ENTITIES WITH HISTORICAL VERSIONS:")
        print("-" * 50)
        cursor.execute("""
            SELECT business_id, COUNT(*) as version_count,
                   MIN(effective_date) as first_version,
                   MAX(effective_date) as latest_version
            FROM customers_scd2 
            GROUP BY business_id
            HAVING COUNT(*) > 1
            ORDER BY version_count DESC, business_id
        """)
        
        multi_version = cursor.fetchall()
        for entity in multi_version:
            print(f"Business ID {entity['business_id']}: {entity['version_count']} versions")
            print(f"   First: {entity['first_version']} | Latest: {entity['latest_version']}")
        
        # Overall summary
        cursor.execute("SELECT COUNT(*) as total FROM customers_scd2")
        total_records = cursor.fetchone()['total']
        
        cursor.execute("SELECT COUNT(*) as current FROM customers_scd2 WHERE is_current = TRUE")
        current_count = cursor.fetchone()['current']
        
        cursor.execute("SELECT COUNT(DISTINCT business_id) as entities FROM customers_scd2")
        entity_count = cursor.fetchone()['entities']
        
        print(f"\nSUMMARY:")
        print(f"Total Records: {total_records}")
        print(f"Current Records: {current_count}")
        print(f"Historical Records: {total_records - current_count}")
        print(f"Business Entities: {entity_count}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")

def show_scd2_best_practices():
    """Show SCD2 best practices for identifying current records"""
    
    print(f"\n" + "="*80)
    print("SCD TYPE 2 - BEST PRACTICES FOR CURRENT RECORD IDENTIFICATION")
    print("="*80)
    
    print(f"""
RULE 1: Always use is_current = TRUE
---------------------------------------
✅ CORRECT: SELECT * FROM table WHERE business_id = 200 AND is_current = TRUE;
❌ WRONG:   SELECT * FROM table WHERE business_id = 200 ORDER BY surrogate_id DESC LIMIT 1;

WHY: is_current flag is the definitive indicator of the active record.

RULE 2: Validate with end_date = '9999-12-31 23:59:59'
-----------------------------------------------------
✅ CORRECT: WHERE is_current = TRUE AND end_date = '9999-12-31 23:59:59'
❌ WRONG:   WHERE end_date IS NULL

WHY: NULL end_date can cause confusion. Use explicit far-future date.

RULE 3: Use effective_date for chronological ordering
---------------------------------------------------
✅ CORRECT: ORDER BY effective_date DESC to get latest first
✅ CORRECT: WHERE effective_date <= CURRENT_DATE() for point-in-time queries

RULE 4: Never rely on surrogate_id for "latest"
----------------------------------------------
❌ WRONG: ORDER BY surrogate_id DESC LIMIT 1
✅ RIGHT: WHERE is_current = TRUE

WHY: Surrogate IDs are just auto-increment keys, not timeline indicators.

RULE 5: Handle multiple current records (data integrity)
-------------------------------------------------------
VALIDATION QUERY:
SELECT business_id, COUNT(*) as current_count 
FROM customers_scd2 
WHERE is_current = TRUE 
GROUP BY business_id 
HAVING COUNT(*) > 1;

-- Should return 0 rows (each business entity should have exactly 1 current record)
    """)

if __name__ == "__main__":
    show_current_scd2_records()
    show_scd2_best_practices()
    
    print(f"\nREADY: Use is_current = TRUE to identify latest records!")
