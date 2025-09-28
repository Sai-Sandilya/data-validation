#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error

print("=== MySQL Default Port Test ===")

def test_default_mysql():
    try:
        # Test connection to default MySQL port
        print("Testing connection to default MySQL port (3306)...")
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='Sandy@123'
        )
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"[OK] Connected to MySQL Server version {db_info}")
            
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print(f"[OK] Current database: {record}")
            
            # List available databases
            cursor.execute("SHOW DATABASES;")
            databases = cursor.fetchall()
            print(f"[OK] Available databases: {[db[0] for db in databases]}")
            
            cursor.close()
            connection.close()
            print("[OK] MySQL connection closed successfully")
            return True
            
    except Error as e:
        print(f"[ERROR] Connection to default MySQL failed: {e}")
        return False

if __name__ == "__main__":
    success = test_default_mysql()
    if success:
        print("\n=== MySQL Test completed successfully! ===")
        print("You can create the source_db and target_db databases manually.")
    else:
        print("\n=== MySQL Test failed. Please check your MySQL installation. ===")
