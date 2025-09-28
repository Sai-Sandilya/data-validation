#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
import random
from datetime import datetime, timedelta
from decimal import Decimal

def add_more_scd_test_data():
    """Add extensive test data for thorough SCD Type 2 and SCD Type 3 testing"""
    
    print("[INFO] Adding MORE comprehensive SCD test data...")
    print("[INFO] This will add 30+ additional scenarios for testing")
    
    # Add comprehensive source data 
    add_comprehensive_source_data()
    
    # Add strategic target data with many differences
    add_comprehensive_target_data()
    
    # Show all test scenarios
    show_comprehensive_test_scenarios()
    
    print("\n[READY] Comprehensive SCD test data added!")
    print("[TEST] Now you have extensive scenarios for both SCD2 and SCD3!")

def add_comprehensive_source_data():
    """Add extensive source data with various test scenarios"""
    print("[ADD] Adding comprehensive source test data...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        customers = []
        base_date = datetime.now() - timedelta(days=15)
        
        # SCENARIO GROUP 1: Employee Promotions (Perfect for SCD Type 2)
        # These will show career progression over time
        promotion_customers = [
            (50, "Alex Senior-Engineer", "alex.senior@techcorp.com", "+1-555-5001", "TECH-HQ", 95000.00, "SENIOR", "2024-01-01", base_date + timedelta(hours=1)),
            (51, "Maria Principal-Architect", "maria.principal@techcorp.com", "+1-555-5002", "TECH-HQ", 120000.00, "PRINCIPAL", "2024-01-15", base_date + timedelta(hours=2)),
            (52, "David VP-Engineering", "david.vp@techcorp.com", "+1-555-5003", "TECH-HQ", 150000.00, "VP", "2024-02-01", base_date + timedelta(hours=3)),
            (53, "Lisa Director-Product", "lisa.director@techcorp.com", "+1-555-5004", "PRODUCT-HQ", 140000.00, "DIRECTOR", "2024-02-15", base_date + timedelta(hours=4)),
            (54, "Tom CTO-Officer", "tom.cto@techcorp.com", "+1-555-5005", "EXEC-FLOOR", 200000.00, "CTO", "2024-03-01", base_date + timedelta(hours=5))
        ]
        
        # SCENARIO GROUP 2: Customer Status Changes (Perfect for SCD Type 3)  
        # These will show simple status transitions
        status_customers = [
            (60, "John Premium-Member", "john.premium@service.com", "+1-555-6001", "PREMIUM-TIER", 5000.00, "PREMIUM", "2024-04-01", base_date + timedelta(hours=10)),
            (61, "Sarah VIP-Customer", "sarah.vip@service.com", "+1-555-6002", "VIP-LOUNGE", 8000.00, "VIP", "2024-04-05", base_date + timedelta(hours=11)),
            (62, "Mike Enterprise-Client", "mike.enterprise@service.com", "+1-555-6003", "ENTERPRISE-WING", 15000.00, "ENTERPRISE", "2024-04-10", base_date + timedelta(hours=12)),
            (63, "Emma Platinum-Elite", "emma.platinum@service.com", "+1-555-6004", "ELITE-CENTER", 25000.00, "PLATINUM", "2024-04-15", base_date + timedelta(hours=13)),
            (64, "Chris Diamond-Executive", "chris.diamond@service.com", "+1-555-6005", "DIAMOND-SUITE", 50000.00, "DIAMOND", "2024-04-20", base_date + timedelta(hours=14))
        ]
        
        # SCENARIO GROUP 3: Geographic Relocations (Good for SCD Type 2)
        # These will show location/regional changes over time
        location_customers = [
            (70, "Anna Global-Manager", "anna.global@worldwide.com", "+1-555-7001", "ASIA-PACIFIC", 85000.00, "GLOBAL", "2024-05-01", base_date + timedelta(hours=20)),
            (71, "Peter International-Lead", "peter.intl@worldwide.com", "+44-555-7002", "EUROPE", 90000.00, "INTERNATIONAL", "2024-05-05", base_date + timedelta(hours=21)),
            (72, "Sofia Regional-Director", "sofia.regional@worldwide.com", "+49-555-7003", "EMEA", 95000.00, "REGIONAL", "2024-05-10", base_date + timedelta(hours=22)),
            (73, "Carlos Americas-VP", "carlos.americas@worldwide.com", "+1-555-7004", "AMERICAS", 110000.00, "REGIONAL-VP", "2024-05-15", base_date + timedelta(hours=23)),
            (74, "Yuki Pacific-Manager", "yuki.pacific@worldwide.com", "+81-555-7005", "PACIFIC", 80000.00, "PACIFIC-MGR", "2024-05-20", base_date + timedelta(hours=24))
        ]
        
        # SCENARIO GROUP 4: Salary Adjustments (Perfect for SCD Type 2)
        # These will show compensation changes over time  
        salary_customers = [
            (80, "Robert Finance-Analyst", "robert.finance@corp.com", "+1-555-8001", "FINANCE", 75000.00, "ANALYST", "2024-06-01", base_date + timedelta(hours=30)),
            (81, "Linda Operations-Manager", "linda.ops@corp.com", "+1-555-8002", "OPERATIONS", 85000.00, "MANAGER", "2024-06-05", base_date + timedelta(hours=31)),
            (82, "James Sales-Director", "james.sales@corp.com", "+1-555-8003", "SALES", 95000.00, "DIRECTOR", "2024-06-10", base_date + timedelta(hours=32)),
            (83, "Kate Marketing-VP", "kate.marketing@corp.com", "+1-555-8004", "MARKETING", 105000.00, "VP", "2024-06-15", base_date + timedelta(hours=33)),
            (84, "Steve Executive-SVP", "steve.exec@corp.com", "+1-555-8005", "EXECUTIVE", 125000.00, "SVP", "2024-06-20", base_date + timedelta(hours=34))
        ]
        
        # SCENARIO GROUP 5: New Hires (Missing records)
        # These exist in source but not target
        new_hire_customers = [
            (90, "Fresh Graduate-Trainee", "fresh.grad@newbie.com", "+1-555-9001", "TRAINING", 50000.00, "TRAINEE", "2024-09-01", base_date + timedelta(days=1)),
            (91, "Recent Junior-Developer", "recent.junior@newbie.com", "+1-555-9002", "DEVELOPMENT", 65000.00, "JUNIOR", "2024-09-02", base_date + timedelta(days=1)),
            (92, "New Associate-Consultant", "new.associate@newbie.com", "+1-555-9003", "CONSULTING", 70000.00, "ASSOCIATE", "2024-09-03", base_date + timedelta(days=1)),
            (93, "Intern Full-Time", "intern.fulltime@newbie.com", "+1-555-9004", "INTERNSHIP", 45000.00, "INTERN", "2024-09-04", base_date + timedelta(days=1)),
            (94, "Entry Business-Analyst", "entry.analyst@newbie.com", "+1-555-9005", "BUSINESS", 60000.00, "ENTRY", "2024-09-05", base_date + timedelta(days=1))
        ]
        
        # Combine all new customers
        customers = promotion_customers + status_customers + location_customers + salary_customers + new_hire_customers
        
        # Insert all source customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name=VALUES(name), email=VALUES(email), phone=VALUES(phone), 
            region=VALUES(region), amount=VALUES(amount), status=VALUES(status),
            join_date=VALUES(join_date), last_login=VALUES(last_login)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Added {len(customers)} new source customers")
        
    except Exception as e:
        print(f"[ERROR] Error adding source data: {e}")

def add_comprehensive_target_data():
    """Add target data with old values to create many difference scenarios"""
    print("[ADD] Adding comprehensive target test data with old values...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        customers = []
        old_date = datetime.now() - timedelta(days=90)
        
        # OLD versions of promotion customers (will show career history)
        old_promotion_customers = [
            (50, "Alex Junior-Developer", "alex.junior@oldtech.com", "+1-555-1001", "DEV-FLOOR", 60000.00, "JUNIOR", "2024-01-01", old_date),
            (51, "Maria Senior-Developer", "maria.senior@oldtech.com", "+1-555-1002", "DEV-FLOOR", 75000.00, "SENIOR", "2024-01-15", old_date),
            (52, "David Team-Lead", "david.lead@oldtech.com", "+1-555-1003", "TEAM-SPACE", 85000.00, "LEAD", "2024-02-01", old_date),
            (53, "Lisa Product-Manager", "lisa.pm@oldtech.com", "+1-555-1004", "PRODUCT-AREA", 90000.00, "MANAGER", "2024-02-15", old_date),
            (54, "Tom Engineering-Director", "tom.director@oldtech.com", "+1-555-1005", "DIRECTOR-OFFICE", 110000.00, "DIRECTOR", "2024-03-01", old_date)
        ]
        
        # OLD versions of status customers (will show status progression)
        old_status_customers = [
            (60, "John Basic-Member", "john.basic@oldservice.com", "+1-555-2001", "BASIC-AREA", 1000.00, "BASIC", "2024-04-01", old_date),
            (61, "Sarah Standard-Customer", "sarah.standard@oldservice.com", "+1-555-2002", "STANDARD-ZONE", 2500.00, "STANDARD", "2024-04-05", old_date),
            (62, "Mike Premium-Client", "mike.premium@oldservice.com", "+1-555-2003", "PREMIUM-SECTION", 7500.00, "PREMIUM", "2024-04-10", old_date),
            (63, "Emma Gold-Member", "emma.gold@oldservice.com", "+1-555-2004", "GOLD-LEVEL", 12000.00, "GOLD", "2024-04-15", old_date),
            (64, "Chris Platinum-Customer", "chris.platinum@oldservice.com", "+1-555-2005", "PLATINUM-TIER", 20000.00, "PLATINUM", "2024-04-20", old_date)
        ]
        
        # OLD versions of location customers (will show relocation history)
        old_location_customers = [
            (70, "Anna Regional-Manager", "anna.regional@oldworld.com", "+1-555-3001", "NORTH-AMERICA", 70000.00, "REGIONAL", "2024-05-01", old_date),
            (71, "Peter Local-Lead", "peter.local@oldworld.com", "+44-555-3002", "UK", 75000.00, "LOCAL", "2024-05-05", old_date),
            (72, "Sofia Area-Director", "sofia.area@oldworld.com", "+49-555-3003", "GERMANY", 80000.00, "AREA", "2024-05-10", old_date),
            (73, "Carlos Country-Manager", "carlos.country@oldworld.com", "+1-555-3004", "USA", 85000.00, "COUNTRY", "2024-05-15", old_date),
            (74, "Yuki Local-Manager", "yuki.local@oldworld.com", "+81-555-3005", "JAPAN", 65000.00, "LOCAL", "2024-05-20", old_date)
        ]
        
        # OLD versions of salary customers (will show compensation history)
        old_salary_customers = [
            (80, "Robert Junior-Analyst", "robert.junior@oldcorp.com", "+1-555-4001", "FINANCE", 55000.00, "JUNIOR", "2024-06-01", old_date),
            (81, "Linda Team-Lead", "linda.lead@oldcorp.com", "+1-555-4002", "OPERATIONS", 65000.00, "LEAD", "2024-06-05", old_date),
            (82, "James Senior-Manager", "james.manager@oldcorp.com", "+1-555-4003", "SALES", 75000.00, "SENIOR", "2024-06-10", old_date),
            (83, "Kate Director-Marketing", "kate.director@oldcorp.com", "+1-555-4004", "MARKETING", 85000.00, "DIRECTOR", "2024-06-15", old_date),
            (84, "Steve VP-Executive", "steve.vp@oldcorp.com", "+1-555-4005", "EXECUTIVE", 95000.00, "VP", "2024-06-20", old_date)
        ]
        
        # Note: IDs 90-94 (new hires) are NOT added to target - they're missing!
        
        # Add some extra target-only records
        target_only_customers = [
            (95, "Legacy Employee-Retired", "legacy.retired@old.com", "+1-555-5001", "LEGACY", 0.00, "RETIRED", "2020-01-01", old_date),
            (96, "Former Contractor-Ended", "former.contractor@old.com", "+1-555-5002", "CONTRACTOR", 0.00, "ENDED", "2021-06-01", old_date),
            (97, "Past Intern-Completed", "past.intern@old.com", "+1-555-5003", "INTERNSHIP", 0.00, "COMPLETED", "2023-08-01", old_date)
        ]
        
        # Combine all target customers
        customers = old_promotion_customers + old_status_customers + old_location_customers + old_salary_customers + target_only_customers
        
        # Insert all target customers
        insert_query = """
            INSERT INTO customers (id, name, email, phone, region, amount, status, join_date, last_login)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            name=VALUES(name), email=VALUES(email), phone=VALUES(phone), 
            region=VALUES(region), amount=VALUES(amount), status=VALUES(status),
            join_date=VALUES(join_date), last_login=VALUES(last_login)
        """
        cursor.executemany(insert_query, customers)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Added {len(customers)} target customers with old values")
        
    except Exception as e:
        print(f"[ERROR] Error adding target data: {e}")

def show_comprehensive_test_scenarios():
    """Display comprehensive test scenarios"""
    print("\n" + "="*80)
    print("COMPREHENSIVE SCD TEST SCENARIOS")
    print("="*80)
    
    print("\nSCD TYPE 2 SCENARIOS (Historical Tracking):")
    print("-" * 50)
    print("📈 CAREER PROGRESSION (IDs 50-54):")
    print("   • Alex: Junior Developer → Senior Engineer ($60K → $95K)")
    print("   • Maria: Senior Developer → Principal Architect ($75K → $120K)")
    print("   • David: Team Lead → VP Engineering ($85K → $150K)")
    print("   • Lisa: Product Manager → Director Product ($90K → $140K)")
    print("   • Tom: Engineering Director → CTO ($110K → $200K)")
    
    print("\n🌍 GEOGRAPHIC MOVES (IDs 70-74):")
    print("   • Anna: North America → Asia Pacific (Regional → Global)")
    print("   • Peter: UK → Europe (Local → International)")
    print("   • Sofia: Germany → EMEA (Area → Regional Director)")
    print("   • Carlos: USA → Americas (Country → VP)")
    print("   • Yuki: Japan → Pacific (Local → Pacific Manager)")
    
    print("\n💰 SALARY ADJUSTMENTS (IDs 80-84):")
    print("   • Robert: $55K → $75K (Junior → Analyst)")
    print("   • Linda: $65K → $85K (Lead → Manager)")
    print("   • James: $75K → $95K (Senior → Director)")
    print("   • Kate: $85K → $105K (Director → VP)")
    print("   • Steve: $95K → $125K (VP → SVP)")
    
    print("\nSCD TYPE 3 SCENARIOS (Previous Value Storage):")
    print("-" * 50)
    print("⭐ STATUS UPGRADES (IDs 60-64):")
    print("   • John: Basic Member → Premium Member ($1K → $5K)")
    print("   • Sarah: Standard → VIP Customer ($2.5K → $8K)")
    print("   • Mike: Premium → Enterprise Client ($7.5K → $15K)")
    print("   • Emma: Gold → Platinum Elite ($12K → $25K)")
    print("   • Chris: Platinum → Diamond Executive ($20K → $50K)")
    
    print("\n📋 MISSING RECORDS (IDs 90-94):")
    print("   • Fresh Graduate, Recent Junior, New Associate")
    print("   • Intern Full-Time, Entry Business Analyst")
    print("   • These exist in SOURCE but not TARGET")
    
    print("\n🗂️  EXTRA RECORDS (IDs 95-97):")
    print("   • Legacy Employee, Former Contractor, Past Intern")
    print("   • These exist in TARGET but not SOURCE")
    
    print(f"\n" + "="*80)
    print("TESTING STRATEGY:")
    print("="*80)
    print("1. 📅 Configure SCD Type 2, sync IDs 50-54, 70-74, 80-84")
    print("   → Should create NEW rows + expire OLD rows")
    print("2. 🔄 Configure SCD Type 3, sync IDs 60-64")
    print("   → Should UPDATE rows + store previous values")
    print("3. 🔍 Use SQL Query to see the different structures")
    print("4. 📊 Compare row counts and column structures")

def verify_comprehensive_test_data():
    """Verify comprehensive test data"""
    try:
        source_conn = mysql.connector.connect(**SOURCE_DB)
        target_conn = mysql.connector.connect(**TARGET_DB)
        
        source_cursor = source_conn.cursor()
        target_cursor = target_conn.cursor()
        
        source_cursor.execute("SELECT COUNT(*) FROM customers")
        source_count = source_cursor.fetchone()[0]
        
        target_cursor.execute("SELECT COUNT(*) FROM customers")
        target_count = target_cursor.fetchone()[0]
        
        print(f"\n[VERIFICATION] Data counts:")
        print(f"   Source: {source_count} customers")
        print(f"   Target: {target_count} customers")
        print(f"   Expected differences: ~25 unmatched + 5 missing + 3 extra")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")

if __name__ == "__main__":
    add_more_scd_test_data()
    verify_comprehensive_test_data()
    
    print("\n[READY] 🚀 COMPREHENSIVE SCD TESTING READY!")
    print("Go to http://localhost:5173 and test extensively!")
