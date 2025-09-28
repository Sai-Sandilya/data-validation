#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error

print("=== MySQL Connection Test for Data Validation ===")

def test_mysql_connections():
    """Test connections to both source and target databases"""
    
    # Test source database connection
    print("\n1. Testing Source Database Connection...")
    try:
        source_conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='source_db',
            user='root',
            password='Sandy@123'
        )
        
        if source_conn.is_connected():
            print("[OK] Connected to source_db successfully")
            
            cursor = source_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM customers")
            source_count = cursor.fetchone()[0]
            print(f"[OK] Source database has {source_count} customers")
            
            # Show sample data
            cursor.execute("SELECT * FROM customers LIMIT 3")
            rows = cursor.fetchall()
            print("[OK] Sample data from source:")
            for row in rows:
                print(f"    {row}")
            
            cursor.close()
            source_conn.close()
            
    except Error as e:
        print(f"[ERROR] Source database connection failed: {e}")
        return False
    
    # Test target database connection
    print("\n2. Testing Target Database Connection...")
    try:
        target_conn = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='target_db',
            user='root',
            password='Sandy@123'
        )
        
        if target_conn.is_connected():
            print("[OK] Connected to target_db successfully")
            
            cursor = target_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM customers")
            target_count = cursor.fetchone()[0]
            print(f"[OK] Target database has {target_count} customers")
            
            # Show sample data
            cursor.execute("SELECT * FROM customers LIMIT 3")
            rows = cursor.fetchall()
            print("[OK] Sample data from target:")
            for row in rows:
                print(f"    {row}")
            
            cursor.close()
            target_conn.close()
            
    except Error as e:
        print(f"[ERROR] Target database connection failed: {e}")
        return False
    
    print(f"\n3. Data Comparison Summary:")
    print(f"    Source DB: {source_count} records")
    print(f"    Target DB: {target_count} records")
    print(f"    Difference: {source_count - target_count} records")
    
    return True

def test_api_endpoints():
    """Test if the API endpoints are working"""
    print("\n4. Testing API Endpoints...")
    
    try:
        import requests
        
        # Test main API endpoint
        response = requests.get("http://localhost:8000", timeout=5)
        if response.status_code == 200:
            print("[OK] Main API endpoint is responding")
        else:
            print(f"[ERROR] Main API returned status {response.status_code}")
            
        # Test database test endpoint
        test_config = {
            "host": "localhost",
            "port": 3306,
            "database": "source_db",
            "user": "root",
            "password": "Sandy@123"
        }
        
        response = requests.post("http://localhost:8000/db/test-connection", 
                               json=test_config, timeout=5)
        if response.status_code == 200:
            print("[OK] Database test endpoint is working")
        else:
            print(f"[ERROR] Database test endpoint returned status {response.status_code}")
            
    except ImportError:
        print("[WARNING] requests module not available, skipping API tests")
    except Exception as e:
        print(f"[ERROR] API test failed: {e}")

if __name__ == "__main__":
    success = test_mysql_connections()
    test_api_endpoints()
    
    if success:
        print("\n=== MySQL Test Completed Successfully! ===")
        print("Your data validation system is ready to use!")
        print("Open http://localhost:3000 in your browser to access the web interface.")
    else:
        print("\n=== MySQL Test Failed ===")
        print("Please check your MySQL configuration and try again.")
