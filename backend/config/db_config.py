# Azure SQL Database Configuration for Databricks + PySpark + Lazy Evaluation
# SOURCE_DB points to source database
SOURCE_DB = {
    "host": "data-validation-server.database.windows.net",
    "port": 1433,
    "database": "data-validation-db",
    "user": "adminuser",
    "password": "Sandy@123",
    "database_type": "sqlserver",
    "jdbc_url": "jdbc:sqlserver://data-validation-server.database.windows.net:1433;databaseName=data-validation-db;encrypt=true;trustServerCertificate=false;loginTimeout=30;",
    "databricks_scope": "azure-sql-source"  # For Databricks secret scope
}

# TARGET_DB points to target database  
TARGET_DB = {
    "host": "data-validation-target-db.database.windows.net",
    "port": 1433,
    "database": "data-validation-target-db",
    "user": "adminuser",
    "password": "Sandy@123",
    "database_type": "sqlserver",
    "jdbc_url": "jdbc:sqlserver://data-validation-target-db.database.windows.net:1433;databaseName=data-validation-target-db;encrypt=true;trustServerCertificate=false;loginTimeout=30;",
    "databricks_scope": "azure-sql-target"  # For Databricks secret scope
}

# PySpark Azure SQL JDBC Configuration
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
JDBC_JAR = "/opt/spark-apps/mssql-jdbc-12.4.2.jre8.jar"

# PySpark Configuration for Azure SQL with Lazy Evaluation
PYSPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "256MB",
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g",
    "spark.driver.maxResultSize": "2g",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.adaptive.autoBroadcastJoinThreshold": "10MB",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5"
}

# Lazy Evaluation Configuration
LAZY_EVALUATION_CONFIG = {
    "chunk_size": 100000,  # Records per chunk
    "max_memory_usage": 0.8,  # 80% of available memory
    "parallelism": 4,  # Number of parallel tasks
    "cache_intermediate_results": True,
    "optimize_joins": True,
    "use_broadcast_join": True,
    "broadcast_threshold": "10MB"
}
