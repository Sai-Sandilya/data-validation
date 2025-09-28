#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error

def check_databases():
    try:
        # Check source database
        print("Checking source_db...")
        source_connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='source_db',
            user='root',
            password='Sandy@123'
        )
        
        source_cursor = source_connection.cursor()
        source_cursor.execute("SHOW TABLES;")
        source_tables = source_cursor.fetchall()
        print(f"Source DB tables: {[table[0] for table in source_tables]}")
        
        source_cursor.close()
        source_connection.close()
        
        # Check target database
        print("Checking target_db...")
        target_connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='target_db',
            user='root',
            password='Sandy@123'
        )
        
        target_cursor = target_connection.cursor()
        target_cursor.execute("SHOW TABLES;")
        target_tables = target_cursor.fetchall()
        print(f"Target DB tables: {[table[0] for table in target_tables]}")
        
        target_cursor.close()
        target_connection.close()
        
        print("\n=== Database check completed! ===")
        return True
        
    except Error as e:
        print(f"[ERROR] Database check failed: {e}")
        return False

if __name__ == "__main__":
    check_databases()
