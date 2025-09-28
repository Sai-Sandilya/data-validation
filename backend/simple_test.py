#!/usr/bin/env python3

import os
import sys

print("=== Simple Spark MySQL Test ===")

try:
    from pyspark.sql import SparkSession
    print("[OK] PySpark imported successfully")
except ImportError as e:
    print(f"[ERROR] PySpark import failed: {e}")
    sys.exit(1)

try:
    # Create Spark session
    spark = SparkSession.builder \
        .appName("MySQLTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print(f"[OK] Spark session created successfully")
    print(f"Spark version: {spark.version}")
    
    # Test basic Spark functionality
    df = spark.range(10)
    print(f"[OK] Basic Spark DataFrame created: {df.count()} rows")
    
    spark.stop()
    print("[OK] Spark session stopped successfully")
    
except Exception as e:
    print(f"[ERROR] Spark session creation failed: {e}")
    sys.exit(1)

print("\n=== Test completed successfully! ===")
