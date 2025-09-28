#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
from datetime import datetime, timedelta
from decimal import Decimal

def setup_scd2_table():
    """Create proper SCD Type 2 table with surrogate keys for true historical tracking"""
    
    print("[INFO] Setting up TRUE SCD Type 2 table with proper structure...")
    print("[INFO] This will create customers_scd2 table for multiple-row tracking")
    
    # Create SCD2 table in both databases
    create_scd2_tables()
    
    # Add SCD2 test data
    create_scd2_source_data()
    create_scd2_target_data()
    
    # Show SCD2 test scenarios
    show_scd2_test_scenarios()
    
    print("\n[READY] SCD Type 2 table ready for testing!")
    print("[COMPARE] Now you can test both approaches:")
    print("   - customers table: SCD Type 3 (single row)")  
    print("   - customers_scd2 table: SCD Type 2 (multiple rows)")

def create_scd2_tables():
    """Create customers_scd2 table with proper SCD Type 2 structure"""
    print("[CREATE] Creating SCD Type 2 tables...")
    
    # Create in source database
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        # Drop and create source table
        cursor.execute("DROP TABLE IF EXISTS customers_scd2")
        cursor.execute("""
            CREATE TABLE customers_scd2 (
                surrogate_id INT AUTO_INCREMENT PRIMARY KEY,
                business_id INT NOT NULL,
                name VARCHAR(150),
                email VARCHAR(150), 
                phone VARCHAR(25),
                region VARCHAR(50),
                amount DECIMAL(12,2),
                status VARCHAR(25),
                join_date DATE,
                last_login DATETIME,
                effective_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                end_date DATETIME DEFAULT '9999-12-31 23:59:59',
                is_current BOOLEAN DEFAULT TRUE,
                record_status VARCHAR(50) DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_business_id (business_id),
                INDEX idx_current (business_id, is_current),
                INDEX idx_effective_date (effective_date)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Source customers_scd2 table created")
        
    except Exception as e:
        print(f"[ERROR] Error creating source SCD2 table: {e}")
    
    # Create in target database  
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        # Drop and create target table
        cursor.execute("DROP TABLE IF EXISTS customers_scd2")
        cursor.execute("""
            CREATE TABLE customers_scd2 (
                surrogate_id INT AUTO_INCREMENT PRIMARY KEY,
                business_id INT NOT NULL,
                name VARCHAR(150),
                email VARCHAR(150),
                phone VARCHAR(25), 
                region VARCHAR(50),
                amount DECIMAL(12,2),
                status VARCHAR(25),
                join_date DATE,
                last_login DATETIME,
                effective_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                end_date DATETIME DEFAULT '9999-12-31 23:59:59',
                is_current BOOLEAN DEFAULT TRUE,
                record_status VARCHAR(50) DEFAULT 'ACTIVE',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_business_id (business_id),
                INDEX idx_current (business_id, is_current),
                INDEX idx_effective_date (effective_date)
            )
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("[OK] Target customers_scd2 table created")
        
    except Exception as e:
        print(f"[ERROR] Error creating target SCD2 table: {e}")

def create_scd2_source_data():
    """Create source data for SCD2 testing"""
    print("[CREATE] Creating SCD2 source data...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        # Current date for effective dates
        current_date = datetime.now()
        
        # SCD2 test records - these are the NEW/CURRENT versions
        scd2_records = [
            # Career progression scenarios (business_id 100-104)
            (100, "Alex Senior-Engineer", "alex.senior@techcorp.com", "+1-555-2001", "TECH-HQ", 95000.00, "SENIOR", "2024-01-01", current_date, current_date),
            (101, "Maria Principal-Architect", "maria.principal@techcorp.com", "+1-555-2002", "TECH-HQ", 120000.00, "PRINCIPAL", "2024-01-15", current_date, current_date),
            (102, "David VP-Engineering", "david.vp@techcorp.com", "+1-555-2003", "EXEC-FLOOR", 150000.00, "VP", "2024-02-01", current_date, current_date),
            (103, "Lisa Director-Product", "lisa.director@techcorp.com", "+1-555-2004", "PRODUCT-HQ", 140000.00, "DIRECTOR", "2024-02-15", current_date, current_date),
            (104, "Tom CTO-Officer", "tom.cto@techcorp.com", "+1-555-2005", "EXEC-SUITE", 200000.00, "CTO", "2024-03-01", current_date, current_date),
            
            # Geographic relocation scenarios (business_id 110-114)
            (110, "Anna Global-Manager", "anna.global@worldwide.com", "+1-555-3001", "ASIA-PACIFIC", 95000.00, "GLOBAL", "2024-04-01", current_date, current_date),
            (111, "Peter International-Lead", "peter.intl@worldwide.com", "+44-555-3002", "EUROPE", 90000.00, "INTERNATIONAL", "2024-04-05", current_date, current_date),
            (112, "Sofia Regional-Director", "sofia.regional@worldwide.com", "+49-555-3003", "EMEA", 105000.00, "REGIONAL", "2024-04-10", current_date, current_date),
            
            # New employees (business_id 120-122) - these will be missing in target
            (120, "Fresh Graduate-Trainee", "fresh.grad@newbie.com", "+1-555-4001", "TRAINING", 50000.00, "TRAINEE", "2024-09-01", current_date, current_date),
            (121, "New Junior-Developer", "junior.dev@newbie.com", "+1-555-4002", "DEVELOPMENT", 65000.00, "JUNIOR", "2024-09-02", current_date, current_date),
        ]
        
        # Insert source records
        insert_query = """
            INSERT INTO customers_scd2 
            (business_id, name, email, phone, region, amount, status, join_date, last_login, effective_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, scd2_records)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(scd2_records)} SCD2 source records")
        
    except Exception as e:
        print(f"[ERROR] Error creating SCD2 source data: {e}")

def create_scd2_target_data():
    """Create target data for SCD2 testing with historical records"""
    print("[CREATE] Creating SCD2 target data with historical records...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        # Historical dates
        old_date = datetime.now() - timedelta(days=90)
        older_date = datetime.now() - timedelta(days=180)
        
        # SCD2 target records - these are the OLD/HISTORICAL versions
        scd2_historical_records = [
            # Historical career progression - business_id 100-104
            # Each person has 1-2 historical records
            
            # Alex (business_id 100) - 2 historical records
            (100, "Alex Intern-Student", "alex.intern@oldtech.com", "+1-555-1001", "INTERN-DESK", 35000.00, "INTERN", "2024-01-01", older_date, older_date, old_date, False, "HISTORICAL"),
            (100, "Alex Junior-Developer", "alex.junior@oldtech.com", "+1-555-1001", "DEV-FLOOR", 60000.00, "JUNIOR", "2024-01-01", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # Maria (business_id 101) - 1 historical record  
            (101, "Maria Senior-Developer", "maria.senior@oldtech.com", "+1-555-1002", "DEV-FLOOR", 75000.00, "SENIOR", "2024-01-15", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # David (business_id 102) - 2 historical records
            (102, "David Developer-I", "david.dev1@oldtech.com", "+1-555-1003", "DEV-AREA", 65000.00, "DEVELOPER", "2024-02-01", older_date, older_date, old_date, False, "HISTORICAL"),
            (102, "David Team-Lead", "david.lead@oldtech.com", "+1-555-1003", "TEAM-SPACE", 85000.00, "LEAD", "2024-02-01", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # Lisa (business_id 103) - 1 historical record
            (103, "Lisa Product-Manager", "lisa.pm@oldtech.com", "+1-555-1004", "PRODUCT-AREA", 90000.00, "MANAGER", "2024-02-15", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # Tom (business_id 104) - 1 historical record
            (104, "Tom Engineering-Director", "tom.director@oldtech.com", "+1-555-1005", "DIRECTOR-OFFICE", 110000.00, "DIRECTOR", "2024-03-01", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # Geographic records - business_id 110-112
            (110, "Anna Regional-Manager", "anna.regional@oldworld.com", "+1-555-2001", "NORTH-AMERICA", 70000.00, "REGIONAL", "2024-04-01", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            (111, "Peter Local-Lead", "peter.local@oldworld.com", "+44-555-2002", "UK", 75000.00, "LOCAL", "2024-04-05", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            (112, "Sofia Area-Director", "sofia.area@oldworld.com", "+49-555-2003", "GERMANY", 80000.00, "AREA", "2024-04-10", old_date, old_date, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # Extra target-only records (business_id 130-131)
            (130, "Legacy Employee-Retired", "legacy.retired@old.com", "+1-555-9001", "LEGACY", 0.00, "RETIRED", "2020-01-01", old_date, old_date, '9999-12-31 23:59:59', True, "LEGACY"),
            (131, "Former Contractor-Ended", "former.contractor@old.com", "+1-555-9002", "CONTRACTOR", 0.00, "ENDED", "2021-06-01", old_date, old_date, '9999-12-31 23:59:59', True, "LEGACY"),
        ]
        
        # Insert target records with explicit values for all columns
        insert_query = """
            INSERT INTO customers_scd2 
            (business_id, name, email, phone, region, amount, status, join_date, last_login, 
             effective_date, end_date, is_current, record_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, scd2_historical_records)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(scd2_historical_records)} SCD2 target historical records")
        
    except Exception as e:
        print(f"[ERROR] Error creating SCD2 target data: {e}")

def show_scd2_test_scenarios():
    """Show SCD2 test scenarios"""
    print("\n" + "="*80)
    print("TRUE SCD TYPE 2 TEST SCENARIOS")
    print("="*80)
    
    print("\nTABLE: customers_scd2")
    print("- Surrogate Key: surrogate_id (auto increment)")
    print("- Business Key: business_id (the actual entity ID)")
    print("- Multiple rows per business entity for historical tracking")
    
    print(f"\nCARREER PROGRESSION SCENARIOS (business_id 100-104):")
    print("-" * 60)
    print("Business_ID 100 (Alex):")
    print("  Row 1: Alex Intern-Student ($35K) [2024-2025] HISTORICAL")
    print("  Row 2: Alex Junior-Developer ($60K) [2025-now] CURRENT") 
    print("  Row 3: Alex Senior-Engineer ($95K) [after sync] NEW")
    print("")
    print("Business_ID 102 (David):")
    print("  Row 1: David Developer-I ($65K) [2024-2025] HISTORICAL")
    print("  Row 2: David Team-Lead ($85K) [2025-now] CURRENT")
    print("  Row 3: David VP-Engineering ($150K) [after sync] NEW")
    
    print(f"\nGEOGRAPHIC RELOCATION (business_id 110-112):")
    print("-" * 60)
    print("Business_ID 110 (Anna):")
    print("  Row 1: Anna Regional-Manager, NORTH-AMERICA ($70K) CURRENT")
    print("  Row 2: Anna Global-Manager, ASIA-PACIFIC ($95K) [after sync] NEW")
    
    print(f"\nMISSING RECORDS (business_id 120-121):")
    print("-" * 60)
    print("These exist in SOURCE but not TARGET:")
    print("  Business_ID 120: Fresh Graduate-Trainee")
    print("  Business_ID 121: New Junior-Developer")
    
    print(f"\nEXTRA RECORDS (business_id 130-131):")
    print("-" * 60)
    print("These exist in TARGET but not SOURCE:")
    print("  Business_ID 130: Legacy Employee-Retired")
    print("  Business_ID 131: Former Contractor-Ended")

def verify_scd2_data():
    """Verify SCD2 table data"""
    print("\n[VERIFY] Verifying SCD2 table data...")
    
    try:
        # Check source
        source_conn = mysql.connector.connect(**SOURCE_DB)
        source_cursor = source_conn.cursor()
        source_cursor.execute("SELECT COUNT(*) FROM customers_scd2")
        source_count = source_cursor.fetchone()[0]
        
        # Check target
        target_conn = mysql.connector.connect(**TARGET_DB)
        target_cursor = target_conn.cursor()
        target_cursor.execute("SELECT COUNT(*) FROM customers_scd2")
        target_count = target_cursor.fetchone()[0]
        
        print(f"Source customers_scd2: {source_count} records")
        print(f"Target customers_scd2: {target_count} records")
        
        # Show sample target data structure
        target_cursor.execute("""
            SELECT business_id, name, status, amount, effective_date, end_date, is_current 
            FROM customers_scd2 
            WHERE business_id = 100 
            ORDER BY effective_date
        """)
        alex_history = target_cursor.fetchall()
        
        print(f"\nSAMPLE: Alex's Career History (business_id 100):")
        for i, record in enumerate(alex_history, 1):
            print(f"  Row {i}: {record[1]} | {record[2]} | ${record[3]} | {record[4]} | Current: {record[6]}")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")

if __name__ == "__main__":
    setup_scd2_table()
    verify_scd2_data()
    
    print("\n[READY] TRUE SCD TYPE 2 TABLE READY! 🚀")
    print("\n[TESTING] Now you can test BOTH approaches:")
    print("1. customers table → SCD Type 3 (single row updates)")
    print("2. customers_scd2 table → TRUE SCD Type 2 (multiple rows per entity)")
    print("\nGo to http://localhost:5173 and compare the behaviors!")
