#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
import random
from datetime import datetime, timedelta
from decimal import Decimal

def setup_scd_test_data():
    """Setup comprehensive test data specifically for testing SCD Type 2 and SCD Type 3"""
    
    print("[INFO] Setting up SCD test data for comprehensive SCD Type 2 vs SCD Type 3 testing...")
    
    # Clear and recreate tables
    clear_and_recreate_tables()
    
    # Create source data with clear test scenarios
    create_scd_source_data()
    
    # Create target data with strategic differences for SCD testing
    create_scd_target_data()
    
    # Show test scenarios
    show_scd_test_scenarios()
    
    print("\n[READY] SCD test data setup complete!")
    print("[TEST] Ready to test both SCD Type 2 and SCD Type 3 strategies")

def clear_and_recreate_tables():
    """Clear existing data and recreate tables with clean structure"""
    print("[CLEAR] Clearing and recreating tables for SCD testing...")
    
    # Clear source database
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY,
                name VARCHAR(150),
                email VARCHAR(150),
                phone VARCHAR(25),
                region VARCHAR(50),
                amount DECIMAL(12,2),
                status VARCHAR(25),
                join_date DATE,
                last_login DATETIME
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Source database recreated")
    except Exception as e:
        print(f"[ERROR] Error recreating source: {e}")
    
    # Clear target database
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("""
            CREATE TABLE customers (
                id INT PRIMARY KEY,
                name VARCHAR(150),
                email VARCHAR(150),
                phone VARCHAR(25),
                region VARCHAR(50),
                amount DECIMAL(12,2),
                status VARCHAR(25),
                join_date DATE,
                last_login DATETIME
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Target database recreated")
    except Exception as e:
        print(f"[ERROR] Error recreating target: {e}")

def create_scd_source_data():
    """Create source data with specific scenarios for SCD testing"""
    print("[CREATE] Creating SCD test source data...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        customers = []
        base_date = datetime.now() - timedelta(days=30)
        
        # Scenario 1: Records that will test SCD Type 2 (full history tracking)
        scd2_customers = [
            (1, "John SCD2-Customer", "john.scd2@newcompany.com", "+1-555-2001", "US-WEST", 15000.00, "PREMIUM", "2024-01-15", base_date + timedelta(hours=1)),
            (2, "Jane SCD2-Executive", "jane.scd2@newdomain.com", "+1-555-2002", "US-EAST", 25000.00, "VIP", "2024-02-20", base_date + timedelta(hours=2)),
            (3, "Bob SCD2-Manager", "bob.scd2@enterprise.com", "+1-555-2003", "CANADA", 20000.00, "ENTERPRISE", "2024-03-10", base_date + timedelta(hours=3)),
            (4, "Alice SCD2-Director", "alice.scd2@corp.com", "+1-555-2004", "UK", 30000.00, "PLATINUM", "2024-04-05", base_date + timedelta(hours=4)),
            (5, "Charlie SCD2-Lead", "charlie.scd2@tech.com", "+1-555-2005", "GERMANY", 18000.00, "GOLD", "2024-05-15", base_date + timedelta(hours=5))
        ]
        
        # Scenario 2: Records that will test SCD Type 3 (previous value tracking)
        scd3_customers = [
            (11, "Sarah SCD3-Analyst", "sarah.scd3@updated.com", "+1-555-3001", "ASIA-PACIFIC", 12000.00, "ACTIVE", "2024-06-01", base_date + timedelta(hours=11)),
            (12, "Mike SCD3-Developer", "mike.scd3@newtech.com", "+1-555-3002", "EUROPE", 14000.00, "PREMIUM", "2024-06-15", base_date + timedelta(hours=12)),
            (13, "Lisa SCD3-Designer", "lisa.scd3@creative.com", "+1-555-3003", "SOUTH-AMERICA", 11000.00, "STANDARD", "2024-07-01", base_date + timedelta(hours=13)),
            (14, "Tom SCD3-Tester", "tom.scd3@quality.com", "+1-555-3004", "AFRICA", 13000.00, "BRONZE", "2024-07-15", base_date + timedelta(hours=14)),
            (15, "Emma SCD3-Writer", "emma.scd3@content.com", "+1-555-3005", "OCEANIA", 10000.00, "SILVER", "2024-08-01", base_date + timedelta(hours=15))
        ]
        
        # Scenario 3: Records with no changes (should remain matched)
        unchanged_customers = [
            (21, "Stable Customer One", "stable1@nochange.com", "+1-555-4001", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", base_date),
            (22, "Stable Customer Two", "stable2@nochange.com", "+1-555-4002", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", base_date),
            (23, "Stable Customer Three", "stable3@nochange.com", "+1-555-4003", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", base_date)
        ]
        
        # Scenario 4: New records (missing in target)
        new_customers = [
            (31, "New Customer Alpha", "alpha@brandnew.com", "+1-555-5001", "NEW-MARKET", 8000.00, "PROSPECT", "2024-09-01", base_date + timedelta(days=1)),
            (32, "New Customer Beta", "beta@brandnew.com", "+1-555-5002", "NEW-MARKET", 9000.00, "PROSPECT", "2024-09-02", base_date + timedelta(days=2)),
            (33, "New Customer Gamma", "gamma@brandnew.com", "+1-555-5003", "NEW-MARKET", 7000.00, "PROSPECT", "2024-09-03", base_date + timedelta(days=3))
        ]
        
        # Combine all customers
        customers = scd2_customers + scd3_customers + unchanged_customers + new_customers
        
        # Insert all source customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(customers)} source customers for SCD testing")
        
    except Exception as e:
        print(f"[ERROR] Error creating source data: {e}")

def create_scd_target_data():
    """Create target data with old values to demonstrate SCD differences"""
    print("[CREATE] Creating SCD test target data with old values...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        customers = []
        old_date = datetime.now() - timedelta(days=60)
        
        # OLD versions of SCD2 test records (these will show historical changes)
        scd2_old_customers = [
            (1, "John OldSCD2-Customer", "john.old@oldcompany.com", "+1-555-1001", "US-CENTRAL", 10000.00, "STANDARD", "2024-01-15", old_date),
            (2, "Jane OldSCD2-Manager", "jane.old@olddomain.com", "+1-555-1002", "US-SOUTH", 15000.00, "GOLD", "2024-02-20", old_date),
            (3, "Bob OldSCD2-Employee", "bob.old@oldenterprise.com", "+1-555-1003", "MEXICO", 12000.00, "BRONZE", "2024-03-10", old_date),
            (4, "Alice OldSCD2-Staff", "alice.old@oldcorp.com", "+1-555-1004", "FRANCE", 18000.00, "SILVER", "2024-04-05", old_date),
            (5, "Charlie OldSCD2-Worker", "charlie.old@oldtech.com", "+1-555-1005", "SPAIN", 14000.00, "STANDARD", "2024-05-15", old_date)
        ]
        
        # OLD versions of SCD3 test records (these will show simple updates)
        scd3_old_customers = [
            (11, "Sarah OldSCD3-Junior", "sarah.old@previous.com", "+1-555-2001", "ASIA", 8000.00, "TRIAL", "2024-06-01", old_date),
            (12, "Mike OldSCD3-Junior", "mike.old@oldtech.com", "+1-555-2002", "EU", 10000.00, "BASIC", "2024-06-15", old_date),
            (13, "Lisa OldSCD3-Intern", "lisa.old@oldcreative.com", "+1-555-2003", "LATAM", 7000.00, "INTERN", "2024-07-01", old_date),
            (14, "Tom OldSCD3-Junior", "tom.old@oldquality.com", "+1-555-2004", "MEA", 9000.00, "JUNIOR", "2024-07-15", old_date),
            (15, "Emma OldSCD3-Trainee", "emma.old@oldcontent.com", "+1-555-2005", "APAC", 6000.00, "TRAINEE", "2024-08-01", old_date)
        ]
        
        # Unchanged customers (identical to source)
        unchanged_customers = [
            (21, "Stable Customer One", "stable1@nochange.com", "+1-555-4001", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", datetime.now() - timedelta(days=30)),
            (22, "Stable Customer Two", "stable2@nochange.com", "+1-555-4002", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", datetime.now() - timedelta(days=30)),
            (23, "Stable Customer Three", "stable3@nochange.com", "+1-555-4003", "STABLE-REGION", 5000.00, "UNCHANGED", "2024-01-01", datetime.now() - timedelta(days=30))
        ]
        
        # Extra records (only in target)
        target_only_customers = [
            (41, "Target Only Customer", "targetonly@old.com", "+1-555-6001", "TARGET-ONLY", 3000.00, "OLD", "2023-12-01", old_date),
            (42, "Legacy Customer", "legacy@deprecated.com", "+1-555-6002", "LEGACY", 2000.00, "DEPRECATED", "2023-11-01", old_date)
        ]
        
        # Combine all target customers
        customers = scd2_old_customers + scd3_old_customers + unchanged_customers + target_only_customers
        
        # Insert all target customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(customers)} target customers for SCD testing")
        
    except Exception as e:
        print(f"[ERROR] Error creating target data: {e}")

def show_scd_test_scenarios():
    """Display the test scenarios for verification"""
    print("\n[SCENARIOS] SCD Test Scenarios Created:")
    
    print("\n📅 SCD TYPE 2 TEST SCENARIOS (IDs 1-5):")
    print("   • John (ID 1): Standard → Premium, Region change, Amount increase")
    print("   • Jane (ID 2): Manager → Executive, Status upgrade, Salary increase")  
    print("   • Bob (ID 3): Employee → Manager, Location change, Promotion")
    print("   • Alice (ID 4): Staff → Director, International move, Major raise")
    print("   • Charlie (ID 5): Worker → Lead, Country change, Status upgrade")
    print("   💡 Expected: New rows created, old rows expired with end_date")
    
    print("\n🔄 SCD TYPE 3 TEST SCENARIOS (IDs 11-15):")
    print("   • Sarah (ID 11): Junior → Analyst, Region expansion, Promotion")
    print("   • Mike (ID 12): Junior → Developer, Status upgrade, Salary increase")
    print("   • Lisa (ID 13): Intern → Designer, Department change, Career growth")
    print("   • Tom (ID 14): Junior → Tester, Region change, Role specialization")
    print("   • Emma (ID 15): Trainee → Writer, Complete role change, Major update")
    print("   💡 Expected: Rows updated in-place, old values in *_previous columns")
    
    print("\n✅ CONTROL SCENARIOS:")
    print("   • IDs 21-23: No changes (should remain matched)")
    print("   • IDs 31-33: New customers (missing in target)")
    print("   • IDs 41-42: Target-only customers (extra in target)")
    
    print("\n[COMPARISON] Key Differences to Observe:")
    print("   📅 SCD2: Creates historical timeline, preserves all changes")
    print("   🔄 SCD3: Simple before/after snapshot, minimal storage")

def verify_scd_test_data():
    """Verify the test data was created correctly"""
    print("\n[VERIFY] Verifying SCD test data...")
    
    try:
        # Check source count
        source_conn = mysql.connector.connect(**SOURCE_DB)
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT COUNT(*) FROM customers")
        source_count = source_cursor.fetchone()[0]
        source_cursor.close()
        source_conn.close()
        
        # Check target count
        target_conn = mysql.connector.connect(**TARGET_DB)
        target_cursor = target_conn.cursor()
        target_cursor.execute("SELECT COUNT(*) FROM customers")
        target_count = target_cursor.fetchone()[0]
        target_cursor.close()
        target_conn.close()
        
        print(f"✅ Source: {source_count} customers")
        print(f"✅ Target: {target_count} customers")
        
        print(f"\n[EXPECTED] After SCD Testing:")
        print(f"   📊 Total differences: ~10 unmatched + 3 missing + 2 extra")
        print(f"   📅 SCD2 will create: New rows + expire old ones")
        print(f"   🔄 SCD3 will create: Updated rows + previous value columns")
        
    except Exception as e:
        print(f"[ERROR] Error verifying data: {e}")

if __name__ == "__main__":
    setup_scd_test_data()
    verify_scd_test_data()
    
    print("\n[INSTRUCTIONS] Testing Steps:")
    print("1. 🌐 Go to http://localhost:5173")
    print("2. 🔧 Configure SCD Type for customers table (try both SCD2 and SCD3)")
    print("3. 👁️ Click eye button to view detailed differences")
    print("4. ✅ Select records (IDs 1-5 for SCD2 test, IDs 11-15 for SCD3 test)")
    print("5. 🚀 Click Sync Selected and observe different behaviors")
    print("6. 🔍 Use SQL Query tab to verify results in target database")
    print("7. 📊 Compare SCD2 vs SCD3 results in the database")
