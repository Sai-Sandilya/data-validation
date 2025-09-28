#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
import random
from datetime import datetime, timedelta
from decimal import Decimal

def setup_enhanced_test_data():
    """Setup enhanced test data with diverse scenarios for comprehensive testing"""
    
    print("[INFO] Setting up enhanced test data for comprehensive validation testing...")
    print("[INFO] This will add more records with various mismatch scenarios")
    
    # Add more diverse source records
    add_enhanced_source_data()
    
    # Add strategic target records with different scenarios
    add_enhanced_target_data()
    
    # Show final summary
    show_data_summary()
    
    print("\n[READY] Enhanced test data setup complete!")
    print("[NEXT] Scenarios available for testing:")
    print("   1. Missing records (source only)")
    print("   2. Extra records (target only)")
    print("   3. Unmatched records (different values)")
    print("   4. Edge cases (NULL values, special characters)")
    print("   5. Data type variations (decimals, dates, long strings)")

def add_enhanced_source_data():
    """Add more diverse records to source database"""
    print("[CREATE] Adding enhanced records to source database...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        # Add records 51-100 to source (will be MISSING in target)
        customers = []
        regions = ['US', 'EU', 'ASIA', 'LATAM', 'AFRICA', 'OCEANIA', 'MIDDLE_EAST']
        statuses = ['ACTIVE', 'PREMIUM', 'TRIAL', 'SUSPENDED', 'VIP', 'BRONZE', 'SILVER', 'GOLD']
        
        for i in range(51, 101):  # 50 new records
            # Create diverse data scenarios
            if i % 10 == 0:
                # Every 10th record has special characters and edge cases
                customer = (
                    i,
                    f"Customer-{i} O'Reilly & Sons",  # Special characters
                    f"customer+{i}@domain-test.co.uk",  # Special email format
                    f"+44-{i:04d}-{random.randint(1000,9999)}",  # International format
                    random.choice(regions),
                    Decimal(str(round(random.uniform(0.01, 50000.99), 2))),  # Wide range
                    random.choice(statuses),
                    (datetime.now() - timedelta(days=random.randint(1, 2000))).date()  # Older dates
                )
            elif i % 7 == 0:
                # Every 7th record has NULL/empty values (where allowed)
                customer = (
                    i,
                    f"Customer_{i:03d}",
                    f"customer{i:03d}@example.com",
                    None,  # NULL phone
                    'UNKNOWN',  # Special region
                    Decimal('0.00'),  # Zero amount
                    'INACTIVE',  # Different status
                    datetime.now().date()  # Recent date
                )
            else:
                # Regular diverse records
                customer = (
                    i,
                    f"Enhanced_Customer_{i:03d}",
                    f"enhanced{i:03d}@testdomain.com",
                    f"+1-{random.randint(100,999)}-{i:04d}",
                    random.choice(regions),
                    Decimal(str(round(random.uniform(10.0, 25000.0), 2))),
                    random.choice(statuses),
                    (datetime.now() - timedelta(days=random.randint(30, 1000))).date()
                )
            
            customers.append(customer)
        
        # Insert all new source records
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Added {len(customers)} enhanced records to source database")
        
    except Exception as e:
        print(f"[ERROR] Error adding source data: {e}")

def add_enhanced_target_data():
    """Add strategic records to target database with various mismatch scenarios"""
    print("[CREATE] Adding strategic records to target database...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        customers = []
        
        # Add records 51-70 to target (will be MATCHED with source)
        for i in range(51, 71):  # 20 records that will match source
            customer = (
                i,
                f"Enhanced_Customer_{i:03d}",  # Will match source
                f"enhanced{i:03d}@testdomain.com",  # Will match source
                f"+1-{500 + i}-{i:04d}",  # Will match source pattern
                'US',  # Will match some source records
                Decimal(str(round(1000.0 + (i * 10), 2))),  # Predictable amounts
                'ACTIVE',  # Common status
                (datetime.now() - timedelta(days=i * 5)).date()  # Predictable dates
            )
            customers.append(customer)
        
        # Add records 71-85 to target with DIFFERENT values (will be UNMATCHED)
        for i in range(71, 86):  # 15 records with mismatches
            customer = (
                i,
                f"OldTarget_Customer_{i:03d}",  # DIFFERENT name
                f"old_target{i:03d}@olddomain.com",  # DIFFERENT email
                f"+44-{i:04d}-9999",  # DIFFERENT phone format
                'EU',  # DIFFERENT region
                Decimal(str(round(100.0 + (i * 5), 2))),  # DIFFERENT amounts
                'TRIAL',  # DIFFERENT status
                (datetime.now() - timedelta(days=i * 15)).date()  # DIFFERENT dates
            )
            customers.append(customer)
        
        # Add records 101-120 to target ONLY (will be EXTRA in target)
        for i in range(101, 121):  # 20 records that don't exist in source
            customer = (
                i,
                f"TargetOnly_Customer_{i:03d}",  # Only in target
                f"target_only{i:03d}@target.com",  # Only in target
                f"+61-{i:04d}-{random.randint(1000,9999)}",  # Australian format
                'OCEANIA',  # Only in target
                Decimal(str(round(random.uniform(500.0, 15000.0), 2))),
                'TARGET_ONLY',  # Special status
                datetime.now().date()  # Current date
            )
            customers.append(customer)
        
        # Insert all target records
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Added {len(customers)} strategic records to target database")
        print("[INFO] Target Data Breakdown:")
        print("   - IDs 51-70: Will MATCH source (20 records)")
        print("   - IDs 71-85: Will be UNMATCHED from source (15 records)")
        print("   - IDs 101-120: EXTRA in target only (20 records)")
        
    except Exception as e:
        print(f"[ERROR] Error adding target data: {e}")

def show_data_summary():
    """Show comprehensive data summary"""
    print("\n[SUMMARY] Final Data Summary:")
    
    try:
        # Get source count and sample
        source_conn = mysql.connector.connect(**SOURCE_DB)
        source_cursor = source_conn.cursor(dictionary=True)
        source_cursor.execute("SELECT COUNT(*) as count FROM customers")
        source_total = source_cursor.fetchone()['count']
        
        # Get target count and sample  
        target_conn = mysql.connector.connect(**TARGET_DB)
        target_cursor = target_conn.cursor(dictionary=True)
        target_cursor.execute("SELECT COUNT(*) as count FROM customers")
        target_total = target_cursor.fetchone()['count']
        
        print(f"   📊 Source Database: {source_total} customers")
        print(f"   📊 Target Database: {target_total} customers")
        
        # Show expected validation results
        print(f"\n[EXPECTED] Validation Results:")
        print(f"   ✅ Matched: ~20-70 records (depending on sync status)")
        print(f"   ⚠️  Unmatched: ~15-30 records (data differences)")
        print(f"   ❌ Missing in Target: ~30-50 records (source only)")
        print(f"   ➕ Extra in Target: ~20 records (target only)")
        
        # Show some sample records for verification
        print(f"\n[SAMPLE] Sample Records for Verification:")
        
        # Sample source record
        source_cursor.execute("SELECT * FROM customers WHERE id = 55 LIMIT 1")
        source_sample = source_cursor.fetchone()
        print(f"   📋 Source ID 55: {source_sample['name']}, {source_sample['email']}")
        
        # Sample target unmatched record
        target_cursor.execute("SELECT * FROM customers WHERE id = 75 LIMIT 1")
        target_sample = target_cursor.fetchone()
        if target_sample:
            print(f"   📋 Target ID 75 (Unmatched): {target_sample['name']}, {target_sample['email']}")
        
        # Sample target only record
        target_cursor.execute("SELECT * FROM customers WHERE id = 105 LIMIT 1")
        target_only = target_cursor.fetchone()
        if target_only:
            print(f"   📋 Target Only ID 105: {target_only['name']}, {target_only['email']}")
        
        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Error showing summary: {e}")

def verify_enhanced_data():
    """Verify the enhanced data was created correctly"""
    print("\n[VERIFY] Verifying enhanced test data...")
    
    try:
        # Check record ranges in both databases
        source_conn = mysql.connector.connect(**SOURCE_DB)
        source_cursor = source_conn.cursor()
        
        target_conn = mysql.connector.connect(**TARGET_DB)
        target_cursor = target_conn.cursor()
        
        # Check various ID ranges
        ranges_to_check = [
            (1, 50, "Original Records"),
            (51, 70, "New Matched Records"),
            (71, 85, "Unmatched Records"),
            (86, 100, "Missing in Target"),
            (101, 120, "Extra in Target")
        ]
        
        for start_id, end_id, description in ranges_to_check:
            source_cursor.execute(f"SELECT COUNT(*) FROM customers WHERE id BETWEEN {start_id} AND {end_id}")
            source_count = source_cursor.fetchone()[0]
            
            target_cursor.execute(f"SELECT COUNT(*) FROM customers WHERE id BETWEEN {start_id} AND {end_id}")
            target_count = target_cursor.fetchone()[0]
            
            print(f"   📊 {description} ({start_id}-{end_id}): Source={source_count}, Target={target_count}")
        
        source_cursor.close()
        source_conn.close()
        target_cursor.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Error verifying data: {e}")

if __name__ == "__main__":
    setup_enhanced_test_data()
    verify_enhanced_data()
    
    print("\n[TESTING] Ready for comprehensive testing!")
    print("[USAGE] Go to http://localhost:5173 and:")
    print("   1. Run validation on customers table")
    print("   2. Check the detailed differences")
    print("   3. Test sync operations")
    print("   4. Download CSV/JSON reports")
    print("   5. Use SQL Query to verify results")
