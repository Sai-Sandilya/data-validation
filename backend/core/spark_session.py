from pyspark.sql import SparkSession
import os

def get_spark():
    # Use PySpark installation (pip installed)
    jar_path = os.path.join(os.path.dirname(__file__), '..', '..', 'spark_jobs', 'mysql-connector-j-8.0.33.jar')
    
    # Check if jar file exists
    if not os.path.exists(jar_path):
        print(f"Warning: MySQL connector JAR not found at {jar_path}")
        print("Continuing without JDBC driver...")
        return (SparkSession.builder
            .appName("DataValidationSync")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())
    
    return (SparkSession.builder
        .appName("DataValidationSync")
        .config("spark.jars", jar_path)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())

