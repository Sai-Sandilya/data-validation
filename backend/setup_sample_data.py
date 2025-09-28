#!/usr/bin/env python3

import mysql.connector
from mysql.connector import Error

def setup_sample_data():
    try:
        # Connect to source database
        print("Setting up sample data in source_db...")
        source_connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='source_db',
            user='root',
            password='Sandy@123'
        )
        
        source_cursor = source_connection.cursor()
        
        # Create customers table in source_db
        source_cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                region VARCHAR(50)
            )
        """)
        
        # Insert sample data
        source_cursor.execute("""
            INSERT IGNORE INTO customers (id, name, email, region) VALUES
            (1, 'John Doe', 'john@example.com', 'US'),
            (2, 'Jane Smith', 'jane@example.com', 'EU'),
            (3, 'Bob Johnson', 'bob@example.com', 'US'),
            (4, 'Alice Brown', 'alice@example.com', 'EU'),
            (5, 'Charlie Wilson', 'charlie@example.com', 'US')
        """)
        
        source_connection.commit()
        print("[OK] Sample data inserted into source_db.customers")
        
        source_cursor.close()
        source_connection.close()
        
        # Connect to target database
        print("Setting up target database...")
        target_connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='target_db',
            user='root',
            password='Sandy@123'
        )
        
        target_cursor = target_connection.cursor()
        
        # Create customers table in target_db (with missing data)
        target_cursor.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                region VARCHAR(50)
            )
        """)
        
        # Insert partial data (missing some records to test sync)
        target_cursor.execute("""
            INSERT IGNORE INTO customers (id, name, email, region) VALUES
            (1, 'John Doe', 'john@example.com', 'US'),
            (2, 'Jane Smith', 'jane@example.com', 'EU'),
            (6, 'Extra Record', 'extra@example.com', 'AS')  -- This record exists only in target
        """)
        
        target_connection.commit()
        print("[OK] Sample data inserted into target_db.customers")
        
        target_cursor.close()
        target_connection.close()
        
        print("\n=== Sample data setup completed successfully! ===")
        print("Source DB has 5 customers, Target DB has 3 customers (with 1 extra)")
        return True
        
    except Error as e:
        print(f"[ERROR] Database setup failed: {e}")
        return False

if __name__ == "__main__":
    setup_sample_data()
