#!/usr/bin/env python3

import mysql.connector
from config.db_config import SOURCE_DB, TARGET_DB
from datetime import datetime, timedelta
from decimal import Decimal

def create_rich_scd2_data():
    """Create rich SCD2 test data with various scenarios"""
    
    print("ADDING COMPREHENSIVE SCD2 TEST DATA")
    print("Creating diverse career progressions and business scenarios...")
    
    # Clear existing data first
    clear_existing_data()
    
    # Add rich source data (current/latest versions)
    create_source_data()
    
    # Add rich target data (historical versions)
    create_target_data()
    
    # Show scenarios
    show_test_scenarios()
    
    print("\nCOMPREHENSIVE SCD2 TEST DATA READY!")
    print("Now you can test various SCD2 scenarios in the frontend!")

def clear_existing_data():
    """Clear existing SCD2 data"""
    print("[CLEAR] Removing old SCD2 data...")
    
    for db_config in [SOURCE_DB, TARGET_DB]:
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM customers_scd2")
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"[INFO] Clear data: {e}")

def create_source_data():
    """Create comprehensive source data with current versions"""
    print("[SOURCE] Creating comprehensive source data...")
    
    try:
        conn = mysql.connector.connect(**SOURCE_DB)
        cursor = conn.cursor()
        
        current_time = datetime.now()
        
        # Comprehensive source data - LATEST/CURRENT versions
        source_data = [
            # TECHNOLOGY COMPANY - Career Progressions (200-203)
            (200, "Emma Senior-Architect", "emma.architect@techcorp.com", "+1-555-8001", "TECH-INNOVATION", 135000.00, "SENIOR_ARCHITECT", "2024-01-01"),
            (201, "Liam Principal-Engineer", "liam.principal@techcorp.com", "+1-555-8002", "ENGINEERING", 140000.00, "PRINCIPAL_ENGINEER", "2024-01-15"),
            (202, "Olivia VP-Technology", "olivia.vp@techcorp.com", "+1-555-8003", "EXECUTIVE", 180000.00, "VP", "2024-02-01"),
            (203, "Noah CTO-Chief", "noah.cto@techcorp.com", "+1-555-8004", "C-SUITE", 250000.00, "CTO", "2024-03-01"),
            
            # GLOBAL CONSULTING - International Moves (210-212)
            (210, "Ava Global-Director", "ava.global@worldconsult.com", "+44-555-9001", "LONDON-HQ", 120000.00, "GLOBAL_DIRECTOR", "2024-04-01"),
            (211, "William Regional-VP", "william.vp@worldconsult.com", "+49-555-9002", "BERLIN-OFFICE", 115000.00, "REGIONAL_VP", "2024-04-15"),
            (212, "Sophia Country-Manager", "sophia.country@worldconsult.com", "+81-555-9003", "TOKYO-BRANCH", 110000.00, "COUNTRY_MANAGER", "2024-05-01"),
            
            # FINANCE SECTOR - Rapid Promotions (220-222)
            (220, "James Investment-Director", "james.investment@financeplus.com", "+1-555-7001", "WALL-STREET", 200000.00, "INVESTMENT_DIRECTOR", "2024-06-01"),
            (221, "Isabella Portfolio-Manager", "isabella.portfolio@financeplus.com", "+1-555-7002", "TRADING-FLOOR", 160000.00, "PORTFOLIO_MANAGER", "2024-06-15"),
            (222, "Benjamin Risk-Officer", "benjamin.risk@financeplus.com", "+1-555-7003", "RISK-MANAGEMENT", 150000.00, "RISK_OFFICER", "2024-07-01"),
            
            # STARTUP ECOSYSTEM - Equity Changes (230-232)
            (230, "Mia Co-Founder", "mia.cofounder@startupx.io", "+1-555-6001", "SILICON-VALLEY", 300000.00, "CO_FOUNDER", "2024-08-01"),
            (231, "Ethan Lead-Developer", "ethan.lead@startupx.io", "+1-555-6002", "DEVELOPMENT", 180000.00, "LEAD_DEVELOPER", "2024-08-15"),
            (232, "Charlotte Product-Owner", "charlotte.product@startupx.io", "+1-555-6003", "PRODUCT", 170000.00, "PRODUCT_OWNER", "2024-09-01"),
            
            # NEW HIRES - Will be Missing in Target (300-302)
            (300, "Oliver Fresh-Graduate", "oliver.grad@newtalent.com", "+1-555-5001", "TRAINING-CENTER", 85000.00, "GRADUATE_TRAINEE", "2024-09-15"),
            (301, "Amelia Junior-Consultant", "amelia.junior@newtalent.com", "+1-555-5002", "CONSULTING", 90000.00, "JUNIOR_CONSULTANT", "2024-09-20"),
            (302, "Henry Associate-Developer", "henry.associate@newtalent.com", "+1-555-5003", "DEVELOPMENT", 95000.00, "ASSOCIATE_DEVELOPER", "2024-09-25"),
        ]
        
        # Insert source records
        insert_query = """
            INSERT INTO customers_scd2 
            (business_id, name, email, phone, region, amount, status, join_date, last_login, effective_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        source_records = []
        for record in source_data:
            business_id, name, email, phone, region, amount, status, join_date = record
            source_records.append((
                business_id, name, email, phone, region, amount, status, 
                join_date, current_time, current_time
            ))
        
        cursor.executemany(insert_query, source_records)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(source_data)} comprehensive source records")
        
    except Exception as e:
        print(f"[ERROR] Source data creation failed: {e}")

def create_target_data():
    """Create comprehensive target data with historical versions"""
    print("[TARGET] Creating comprehensive target data with rich history...")
    
    try:
        conn = mysql.connector.connect(**TARGET_DB)
        cursor = conn.cursor()
        
        # Historical dates for progression
        very_old = datetime.now() - timedelta(days=365)  # 1 year ago
        old = datetime.now() - timedelta(days=180)       # 6 months ago  
        recent = datetime.now() - timedelta(days=90)     # 3 months ago
        
        # Rich historical target data with career progressions
        target_data = [
            # EMMA'S CAREER (business_id 200) - 3 progression steps
            (200, "Emma Junior-Developer", "emma.junior@oldtech.com", "+1-555-1001", "DEV-TEAM", 65000.00, "JUNIOR_DEVELOPER", "2024-01-01", very_old, very_old, old, False, "EXPIRED"),
            (200, "Emma Mid-Developer", "emma.mid@oldtech.com", "+1-555-1001", "DEV-TEAM", 80000.00, "MID_DEVELOPER", "2024-01-01", old, old, recent, False, "EXPIRED"), 
            (200, "Emma Senior-Developer", "emma.senior@oldtech.com", "+1-555-1001", "SENIOR-DEV", 100000.00, "SENIOR_DEVELOPER", "2024-01-01", recent, recent, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # LIAM'S CAREER (business_id 201) - 2 progression steps
            (201, "Liam Junior-Engineer", "liam.junior@oldtech.com", "+1-555-1002", "ENGINEERING", 70000.00, "JUNIOR_ENGINEER", "2024-01-15", very_old, very_old, old, False, "EXPIRED"),
            (201, "Liam Senior-Engineer", "liam.senior@oldtech.com", "+1-555-1002", "ENGINEERING", 95000.00, "SENIOR_ENGINEER", "2024-01-15", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # OLIVIA'S CAREER (business_id 202) - 1 current record
            (202, "Olivia Team-Lead", "olivia.lead@oldtech.com", "+1-555-1003", "TEAM-LEADERSHIP", 110000.00, "TEAM_LEAD", "2024-02-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # NOAH'S CAREER (business_id 203) - 1 current record
            (203, "Noah Engineering-Director", "noah.director@oldtech.com", "+1-555-1004", "DIRECTOR-OFFICE", 180000.00, "ENGINEERING_DIRECTOR", "2024-03-01", recent, recent, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # AVA'S INTERNATIONAL JOURNEY (business_id 210) - 2 steps
            (210, "Ava Regional-Manager", "ava.regional@oldworld.com", "+1-555-2001", "NEW-YORK", 85000.00, "REGIONAL_MANAGER", "2024-04-01", very_old, very_old, old, False, "EXPIRED"),
            (210, "Ava Senior-Manager", "ava.senior@oldworld.com", "+33-555-2001", "PARIS-OFFICE", 95000.00, "SENIOR_MANAGER", "2024-04-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # WILLIAM'S CAREER (business_id 211) - 1 current
            (211, "William Local-Consultant", "william.local@oldconsult.com", "+1-555-2002", "LOCAL-OFFICE", 75000.00, "LOCAL_CONSULTANT", "2024-04-15", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # SOPHIA'S CAREER (business_id 212) - 1 current
            (212, "Sophia Area-Coordinator", "sophia.area@oldconsult.com", "+65-555-2003", "SINGAPORE", 80000.00, "AREA_COORDINATOR", "2024-05-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # FINANCE SECTOR PROGRESSIONS
            # JAMES (business_id 220) - 2 steps
            (220, "James Junior-Analyst", "james.analyst@oldfinance.com", "+1-555-3001", "ANALYST-DESK", 90000.00, "JUNIOR_ANALYST", "2024-06-01", very_old, very_old, old, False, "EXPIRED"),
            (220, "James Senior-Analyst", "james.senior@oldfinance.com", "+1-555-3001", "SENIOR-DESK", 120000.00, "SENIOR_ANALYST", "2024-06-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # ISABELLA (business_id 221) - 1 current
            (221, "Isabella Trader-Associate", "isabella.trader@oldfinance.com", "+1-555-3002", "TRADING-ASSOCIATE", 100000.00, "TRADER_ASSOCIATE", "2024-06-15", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # BENJAMIN (business_id 222) - 1 current
            (222, "Benjamin Risk-Analyst", "benjamin.analyst@oldfinance.com", "+1-555-3003", "RISK-ANALYSIS", 95000.00, "RISK_ANALYST", "2024-07-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # STARTUP ECOSYSTEM
            # MIA (business_id 230) - 1 current
            (230, "Mia Technical-Lead", "mia.tech@earlystartup.com", "+1-555-4001", "GARAGE-OFFICE", 150000.00, "TECHNICAL_LEAD", "2024-08-01", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # ETHAN (business_id 231) - 1 current
            (231, "Ethan Full-Stack-Developer", "ethan.fullstack@earlystartup.com", "+1-555-4002", "DEVELOPMENT", 110000.00, "FULLSTACK_DEVELOPER", "2024-08-15", old, old, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # CHARLOTTE (business_id 232) - 1 current
            (232, "Charlotte Product-Analyst", "charlotte.analyst@earlystartup.com", "+1-555-4003", "PRODUCT", 95000.00, "PRODUCT_ANALYST", "2024-09-01", recent, recent, '9999-12-31 23:59:59', True, "CURRENT"),
            
            # LEGACY EMPLOYEES (Extra in Target)
            (400, "Legacy Senior-Manager", "legacy.manager@oldcompany.com", "+1-555-9001", "LEGACY-DEPARTMENT", 120000.00, "SENIOR_MANAGER", "2020-01-01", old, old, '9999-12-31 23:59:59', True, "LEGACY"),
            (401, "Former VP-Operations", "former.vp@oldcompany.com", "+1-555-9002", "OPERATIONS", 180000.00, "VP_OPERATIONS", "2021-01-01", old, old, '9999-12-31 23:59:59', True, "LEGACY"),
            (402, "Retired CTO-Emeritus", "retired.cto@oldcompany.com", "+1-555-9003", "EMERITUS", 0.00, "RETIRED", "2019-01-01", old, old, '9999-12-31 23:59:59', True, "RETIRED"),
        ]
        
        # Insert target records with explicit values for all columns
        insert_query = """
            INSERT INTO customers_scd2 
            (business_id, name, email, phone, region, amount, status, join_date, last_login, 
             effective_date, end_date, is_current, record_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_query, target_data)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"[OK] Created {len(target_data)} comprehensive target historical records")
        
    except Exception as e:
        print(f"[ERROR] Target data creation failed: {e}")

def show_test_scenarios():
    """Show comprehensive SCD2 test scenarios"""
    print("\n" + "="*70)
    print("COMPREHENSIVE SCD2 TEST SCENARIOS")
    print("="*70)
    
    print(f"\nTECHNOLOGY COMPANY - Career Progressions:")
    print("Emma (200): Junior -> Mid -> Senior -> [SYNC] -> Senior-Architect ($135K)")
    print("Liam (201): Junior -> Senior -> [SYNC] -> Principal-Engineer ($140K)")
    print("Olivia (202): Team-Lead -> [SYNC] -> VP-Technology ($180K)")
    print("Noah (203): Engineering-Director -> [SYNC] -> CTO-Chief ($250K)")
    
    print(f"\nGLOBAL CONSULTING - International Moves:")
    print("Ava (210): Regional-Manager (NY) -> Senior-Manager (Paris) -> [SYNC] -> Global-Director (London) ($120K)")
    print("William (211): Local-Consultant -> [SYNC] -> Regional-VP (Berlin) ($115K)")  
    print("Sophia (212): Area-Coordinator (Singapore) -> [SYNC] -> Country-Manager (Tokyo) ($110K)")
    
    print(f"\nFINANCE SECTOR - Wall Street Success:")
    print("James (220): Junior-Analyst -> Senior-Analyst -> [SYNC] -> Investment-Director ($200K)")
    print("Isabella (221): Trader-Associate -> [SYNC] -> Portfolio-Manager ($160K)")
    print("Benjamin (222): Risk-Analyst -> [SYNC] -> Risk-Officer ($150K)")
    
    print(f"\nSTARTUP ECOSYSTEM - Equity & Growth:")
    print("Mia (230): Technical-Lead -> [SYNC] -> Co-Founder ($300K + Equity)")
    print("Ethan (231): Full-Stack-Developer -> [SYNC] -> Lead-Developer ($180K)")
    print("Charlotte (232): Product-Analyst -> [SYNC] -> Product-Owner ($170K)")
    
    print(f"\nNEW HIRES - Missing in Target (Fresh Talent):")
    print("Oliver (300): Fresh Graduate-Trainee ($85K) - NEW HIRE")
    print("Amelia (301): Junior-Consultant ($90K) - NEW HIRE")
    print("Henry (302): Associate-Developer ($95K) - NEW HIRE")
    
    print(f"\nLEGACY EMPLOYEES - Extra in Target Only:")
    print("Legacy Senior-Manager (400): Still in old system")
    print("Former VP-Operations (401): Historical record")
    print("Retired CTO-Emeritus (402): Emeritus status")
    
    print(f"\nEXPECTED SYNC RESULTS:")
    print("Total Source Records: 16")
    print("Career Progressions: 13 (promotions, raises, relocations)")
    print("Missing in Target: 3 (new hires)")  
    print("Extra in Target: 3 (legacy employees)")
    print("Total Historical Rows: ~25+ (rich career timelines)")

def verify_data():
    """Verify comprehensive SCD2 data"""
    print("\n[VERIFY] Comprehensive SCD2 data verification...")
    
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
        
        # Check current records in target
        target_cursor.execute("SELECT COUNT(*) FROM customers_scd2 WHERE is_current = TRUE")
        current_count = target_cursor.fetchone()[0]
        
        print(f"Source Records: {source_count}")
        print(f"Target Records: {target_count}")
        print(f"Current Records: {current_count}")
        print(f"Historical Records: {target_count - current_count}")
        
        source_cursor.close()
        target_cursor.close()
        source_conn.close()
        target_conn.close()
        
    except Exception as e:
        print(f"[ERROR] Verification failed: {e}")

if __name__ == "__main__":
    create_rich_scd2_data()
    verify_data()
    
    print("\nCOMPREHENSIVE SCD2 DATA READY FOR TESTING!")
    print("Go to http://localhost:5173 and test SCD Type 2 scenarios!")
    print("Select customers_scd2 table and validate to see all the career progressions!")
