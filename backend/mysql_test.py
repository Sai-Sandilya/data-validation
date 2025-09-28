#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error

print("=== MySQL Connection Test ===")

def test_mysql_connection():
    try:
        # Test connection to source database
        print("Testing connection to source database (port 3307)...")
        connection = mysql.connector.connect(
            host='localhost',
            port=3307,
            database='source_db',
            user='root',
            password='Sandy@123'
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"[OK] Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print(f"[OK] You're connected to database: {record}")
            
            cursor.close()
            connection.close()
            print("[OK] MySQL connection closed successfully")
            return True
            
    except Error as e:
        print(f"[ERROR] Connection to source database failed: {e}")
    
    try:
        # Test connection to target database
        print("\nTesting connection to target database (port 3308)...")
        connection = mysql.connector.connect(
            host='localhost',
            port=3308,
            database='target_db',
            user='root',
            password='Sandy@123'
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"[OK] Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print(f"[OK] You're connected to database: {record}")
            
            cursor.close()
            connection.close()
            print("[OK] MySQL connection closed successfully")
            return True
            
    except Error as e:
        print(f"[ERROR] Connection to target database failed: {e}")
        return False

if __name__ == "__main__":
    success = test_mysql_connection()
    if success:
        print("\n=== MySQL Test completed successfully! ===")
    else:
        print("\n=== MySQL Test failed. Please check your MySQL setup. ===")
