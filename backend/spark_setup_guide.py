#!/usr/bin/env python3

import os
import sys

print("=== Spark Setup Guide ===")
print()

# Check if Spark is installed
spark_home = os.environ.get('SPARK_HOME')
print(f"SPARK_HOME environment variable: {spark_home}")

if spark_home and os.path.exists(spark_home):
    print(f"[OK] Spark directory exists: {spark_home}")
    
    # Check for spark-submit
    spark_submit = os.path.join(spark_home, 'bin', 'spark-submit.cmd')
    if os.path.exists(spark_submit):
        print("[OK] spark-submit found")
    else:
        print("[WARNING] spark-submit not found")
        
    # Check for Spark jars
    jars_dir = os.path.join(spark_home, 'jars')
    if os.path.exists(jars_dir):
        print(f"[OK] Spark jars directory exists")
        jar_files = [f for f in os.listdir(jars_dir) if f.endswith('.jar')]
        print(f"[INFO] Found {len(jar_files)} jar files")
    else:
        print("[WARNING] Spark jars directory not found")
else:
    print("[ERROR] SPARK_HOME not set or directory doesn't exist")
    print()
    print("To install Spark:")
    print("1. Download Apache Spark 3.3.4 from: https://spark.apache.org/downloads.html")
    print("2. Extract to C:\\spark")
    print("3. Set environment variables:")
    print("   - SPARK_HOME=C:\\spark")
    print("   - Add C:\\spark\\bin to PATH")
    print("   - JAVA_HOME should point to your Java installation")
    print()

# Check Java
java_home = os.environ.get('JAVA_HOME')
print(f"JAVA_HOME environment variable: {java_home}")

if java_home and os.path.exists(java_home):
    print("[OK] Java directory exists")
else:
    print("[WARNING] JAVA_HOME not set or directory doesn't exist")

# Check current PATH
path_dirs = os.environ.get('PATH', '').split(';')
spark_in_path = any('spark' in dir.lower() for dir in path_dirs)
print(f"Spark in PATH: {spark_in_path}")

print()
print("=== Alternative: Use PySpark without Spark installation ===")
print("If you want to test the functionality without Spark:")
print("1. The MySQL connections are working (as shown in previous test)")
print("2. You can use the web interface at http://localhost:3000")
print("3. The API endpoints are functional")
print("4. For actual Spark functionality, you'll need to install Spark separately")
