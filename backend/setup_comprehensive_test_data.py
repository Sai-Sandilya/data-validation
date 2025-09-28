import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
import random
from datetime import datetime, timedelta

def setup_comprehensive_test_data():
    """Setup 50 rows of test data with mixed scenarios for testing SCD Type 3"""
    
    # Clear existing data
    clear_existing_data()
    
    # Create source data (50 records)
    create_source_data()
    
    # Create target data (30 records with some differences)
    create_target_data()
    
    print("[OK] Comprehensive test data setup complete!")
    print("[DATA] Data Distribution:")
    print("   - Source: 50 customers")
    print("   - Target: 30 customers (20 missing, 10 with differences)")
    print("   - Expected after sync:")
    print("     * Case 1 (Missing): 20 new inserts")
    print("     * Case 2 (Unmatched): 10 SCD Type 3 updates")
    print("     * Matched: 20 unchanged records")

def clear_existing_data():
    """Clear existing test data"""
    print("[CLEAR] Clearing existing data...")
    
    # Clear source database
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(150),
                phone VARCHAR(20),
                region VARCHAR(50),
                amount DECIMAL(10,2),
                status VARCHAR(20),
                join_date DATE
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Source database cleared and recreated")
    except Exception as e:
        print(f"[ERROR] Error clearing source: {e}")
    
    # Clear target database
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(150),
                phone VARCHAR(20),
                region VARCHAR(50),
                amount DECIMAL(10,2),
                status VARCHAR(20),
                join_date DATE
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Target database cleared and recreated")
    except Exception as e:
        print(f"[ERROR] Error clearing target: {e}")

def create_source_data():
    """Create 50 customers in source database"""
    print("[CREATE] Creating source data (50 customers)...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        # Generate 50 diverse customer records
        customers = []
        regions = ['US', 'EU', 'ASIA', 'LATAM', 'AFRICA']
        statuses = ['ACTIVE', 'PREMIUM', 'TRIAL', 'SUSPENDED']
        
        for i in range(1, 51):
            customer = (
                i,  # id
                f"Customer_{i:03d}",  # name
                f"customer{i:03d}@example.com",  # email
                f"+1-555-{i:04d}",  # phone
                random.choice(regions),  # region
                round(random.uniform(100.0, 10000.0), 2),  # amount
                random.choice(statuses),  # status
                (datetime.now() - timedelta(days=random.randint(1, 365))).date()  # join_date
            )
            customers.append(customer)
        
        # Insert all customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(customers)} customers in source database")
        
    except Exception as e:
        print(f"[ERROR] Error creating source data: {e}")

def create_target_data():
    """Create 30 customers in target database with strategic differences"""
    print("[CREATE] Creating target data (30 customers with differences)...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        customers = []
        regions = ['US', 'EU', 'ASIA', 'LATAM', 'AFRICA']
        statuses = ['ACTIVE', 'PREMIUM', 'TRIAL', 'SUSPENDED']
        
        # Create 30 customers (IDs 1-30)
        # This leaves IDs 31-50 missing (Case 1: Missing records)
        for i in range(1, 31):
            if i <= 20:
                # IDs 1-20: Identical to source (will be MATCHED)
                customer = (
                    i,  # id
                    f"Customer_{i:03d}",  # name - same as source
                    f"customer{i:03d}@example.com",  # email - same as source
                    f"+1-555-{i:04d}",  # phone - same as source
                    regions[i % len(regions)],  # region - same pattern
                    round(100.0 + (i * 100), 2),  # amount - predictable pattern
                    statuses[i % len(statuses)],  # status - same pattern
                    (datetime.now() - timedelta(days=i * 10)).date()  # join_date - predictable
                )
            else:
                # IDs 21-30: Different from source (Case 2: Unmatched records)
                customer = (
                    i,  # id
                    f"OldCustomer_{i:03d}",  # name - DIFFERENT (will trigger SCD Type 3)
                    f"old_customer{i:03d}@oldmail.com",  # email - DIFFERENT
                    f"+1-999-{i:04d}",  # phone - DIFFERENT
                    regions[(i + 2) % len(regions)],  # region - DIFFERENT
                    round(50.0 + (i * 50), 2),  # amount - DIFFERENT (will be preserved in amount_previous)
                    statuses[(i + 1) % len(statuses)],  # status - DIFFERENT
                    (datetime.now() - timedelta(days=i * 20)).date()  # join_date - DIFFERENT
                )
            
            customers.append(customer)
        
        # Insert all customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(customers)} customers in target database")
        print("[INFO] Target Data Breakdown:")
        print("   - IDs 1-20: Identical to source (MATCHED)")
        print("   - IDs 21-30: Different from source (UNMATCHED - will trigger SCD Type 3)")
        print("   - IDs 31-50: Missing from target (MISSING - will be inserted)")
        
    except Exception as e:
        print(f"[ERROR] Error creating target data: {e}")

def verify_test_data():
    """Verify the test data setup"""
    print("\n[VERIFY] Verifying test data setup...")
    
    try:
        # Check source count
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        source_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        # Check target count
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        target_count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        print(f"[OK] Source database: {source_count} customers")
        print(f"[OK] Target database: {target_count} customers")
        
        # Show sample differences for verification
        print("\n[SAMPLE] Sample Data for Verification:")
        show_sample_differences()
        
    except Exception as e:
        print(f"[ERROR] Error verifying data: {e}")

def show_sample_differences():
    """Show sample records to verify the differences"""
    try:
        # Get sample records from both databases
        source_conn = mysql.connector.connect(**SOURCE_DB)
        target_conn = mysql.connector.connect(**TARGET_DB)
        
        source_cursor = source_conn.cursor(dictionary=True)
        target_cursor = target_conn.cursor(dictionary=True)
        
        # Show a matched record (ID 5)
        source_cursor.execute("SELECT * FROM customers WHERE id = 5")
        source_record = source_cursor.fetchone()
        
        target_cursor.execute("SELECT * FROM customers WHERE id = 5")
        target_record = target_cursor.fetchone()
        
        print("\n[MATCHED] MATCHED Record (ID 5):")
        print(f"   Source: {source_record}")
        print(f"   Target: {target_record}")
        
        # Show an unmatched record (ID 25)
        source_cursor.execute("SELECT * FROM customers WHERE id = 25")
        source_record = source_cursor.fetchone()
        
        target_cursor.execute("SELECT * FROM customers WHERE id = 25")
        target_record = target_cursor.fetchone()
        
        print("\n[UNMATCHED] UNMATCHED Record (ID 25) - Will trigger SCD Type 3:")
        print(f"   Source: {source_record}")
        print(f"   Target: {target_record}")
        
        # Show a missing record (ID 45)
        source_cursor.execute("SELECT * FROM customers WHERE id = 45")
        source_record = source_cursor.fetchone()
        
        target_cursor.execute("SELECT * FROM customers WHERE id = 45")
        target_record = target_cursor.fetchone()
        
        print("\n[MISSING] MISSING Record (ID 45) - Will be inserted:")
        print(f"   Source: {source_record}")
        print(f"   Target: {target_record}")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Error showing sample differences: {e}")

if __name__ == "__main__":
    setup_comprehensive_test_data()
    verify_test_data()
    
    print("\n[READY] Ready for testing!")
    print("[NEXT] Next steps:")
    print("   1. Run comparison to see the differences")
    print("   2. Use eye button to view detailed comparison")
    print("   3. Select records and sync to test SCD Type 3")
    print("   4. Use SQL Query interface to verify results")
