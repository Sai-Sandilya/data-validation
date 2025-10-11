"""
Spark Lazy Evaluation System for 1B+ Records
Handles massive datasets with chunked processing and lazy transformations
"""

from typing import Dict, List, Any, Iterator, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import psutil
import time

logger = logging.getLogger(__name__)

class SparkLazyEvaluator:
    """Spark Lazy Evaluation for handling 1B+ records efficiently"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.chunk_size = 1_000_000  # 1M records per chunk
        self.max_memory_usage = 0.8  # 80% of available memory
        self.performance_metrics = {}
        
    def get_optimal_chunk_size(self, total_records: int) -> int:
        """Calculate optimal chunk size based on available memory and record count"""
        try:
            # Get available memory
            available_memory = psutil.virtual_memory().available
            memory_per_record = 1024  # Estimated bytes per record
            
            # Calculate chunk size based on memory
            memory_based_chunk = int((available_memory * self.max_memory_usage) / memory_per_record)
            
            # Calculate chunk size based on total records (aim for 1000 chunks max)
            record_based_chunk = max(1_000_000, total_records // 1000)
            
            # Use the smaller of the two
            optimal_chunk = min(memory_based_chunk, record_based_chunk)
            
            # Ensure minimum chunk size
            return max(100_000, optimal_chunk)
            
        except Exception as e:
            logger.warning(f"Could not calculate optimal chunk size: {e}")
            return self.chunk_size
    
    def create_lazy_dataframe(self, source_config: Dict, target_config: Dict, 
                            table_name: str, filters: Dict = None) -> Tuple[DataFrame, DataFrame]:
        """Create lazy DataFrames for source and target tables"""
        try:
            # Source DataFrame with lazy evaluation
            source_df = self._create_lazy_table_df(source_config, table_name, filters)
            
            # Target DataFrame with lazy evaluation  
            target_df = self._create_lazy_table_df(target_config, table_name, filters)
            
            return source_df, target_df
            
        except Exception as e:
            logger.error(f"Failed to create lazy DataFrames: {e}")
            raise
    
    def _create_lazy_table_df(self, config: Dict, table_name: str, filters: Dict = None) -> DataFrame:
        """Create a lazy DataFrame from Azure SQL database table using Databricks JDBC"""
        try:
            # Use Databricks JDBC connection for Azure SQL
            jdbc_url = config.get('jdbc_url', f"jdbc:sqlserver://{config['host']}:{config['port']};databaseName={config['database']};encrypt=true;trustServerCertificate=false;loginTimeout=30;")
            
            # Create DataFrame with lazy evaluation using Databricks JDBC
            df = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", config['user']) \
                .option("password", config['password']) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option("fetchsize", "10000") \
                .option("batchsize", "10000") \
                .option("isolationLevel", "READ_COMMITTED") \
                .option("numPartitions", "4") \
                .load()
            
            # Apply filters if provided
            if filters:
                for column, value in filters.items():
                    if value != "ALL":
                        df = df.filter(col(column) == value)
            
            # Optimize for large datasets with lazy evaluation
            df = df.repartition(4)
            
            logger.info(f"Created PySpark lazy DataFrame for {table_name} with Databricks JDBC")
            return df
            
        except Exception as e:
            logger.error(f"Failed to create PySpark lazy DataFrame for {table_name}: {e}")
            raise
    
    def lazy_hash_comparison(self, source_df: DataFrame, target_df: DataFrame, 
                           business_keys: List[str]) -> DataFrame:
        """Perform lazy hash comparison for large datasets"""
        try:
            start_time = time.time()
            
            # Add hash columns lazily
            source_with_hash = source_df.withColumn(
                "source_hash", 
                sha2(concat_ws("|", *[col(c) for c in business_keys]), 256)
            )
            
            target_with_hash = target_df.withColumn(
                "target_hash", 
                sha2(concat_ws("|", *[col(c) for c in business_keys]), 256)
            )
            
            # Join on business keys with lazy evaluation
            comparison_df = source_with_hash.alias("source") \
                .join(
                    target_with_hash.alias("target"),
                    business_keys,
                    "full_outer"
                ) \
                .withColumn("hash_match", 
                    when(col("source_hash") == col("target_hash"), "MATCH")
                    .when(col("source_hash").isNull(), "MISSING_IN_SOURCE")
                    .when(col("target_hash").isNull(), "MISSING_IN_TARGET")
                    .otherwise("MISMATCH")
                )
            
            # Cache the result for multiple operations
            comparison_df.cache()
            
            # Record performance metrics
            self.performance_metrics['hash_comparison_time'] = time.time() - start_time
            
            return comparison_df
            
        except Exception as e:
            logger.error(f"Lazy hash comparison failed: {e}")
            raise
    
    def lazy_scd2_sync(self, source_df: DataFrame, target_df: DataFrame, 
                      business_keys: List[str], scd_columns: List[str]) -> DataFrame:
        """Implement SCD Type 2 with lazy evaluation for large datasets"""
        try:
            start_time = time.time()
            
            # Get current timestamp
            current_timestamp = current_timestamp()
            
            # Create SCD2 logic with lazy evaluation
            scd2_df = source_df.alias("source") \
                .join(
                    target_df.alias("target"),
                    business_keys,
                    "left"
                ) \
                .withColumn("is_current", 
                    when(col("target.is_current").isNull(), True)
                    .otherwise(False)
                ) \
                .withColumn("effective_date", current_timestamp) \
                .withColumn("end_date", 
                    when(col("target.is_current") == True, current_timestamp)
                    .otherwise(None)
                ) \
                .withColumn("record_status", 
                    when(col("target.is_current").isNull(), "NEW")
                    .when(col("source_hash") != col("target_hash"), "UPDATED")
                    .otherwise("UNCHANGED")
                )
            
            # Filter only changed records for efficiency
            changed_records = scd2_df.filter(
                col("record_status").isin(["NEW", "UPDATED"])
            )
            
            # Record performance metrics
            self.performance_metrics['scd2_sync_time'] = time.time() - start_time
            
            return changed_records
            
        except Exception as e:
            logger.error(f"Lazy SCD2 sync failed: {e}")
            raise
    
    def lazy_scd3_sync(self, source_df: DataFrame, target_df: DataFrame, 
                      business_keys: List[str], scd_columns: List[str]) -> DataFrame:
        """Implement SCD Type 3 with lazy evaluation for large datasets"""
        try:
            start_time = time.time()
            
            # Create SCD3 logic with lazy evaluation
            scd3_df = source_df.alias("source") \
                .join(
                    target_df.alias("target"),
                    business_keys,
                    "left"
                ) \
                .withColumn("last_updated", current_timestamp()) \
                .withColumn("record_status",
                    when(col("target.business_key").isNull(), "NEW")
                    .when(col("source_hash") != col("target_hash"), "UPDATED")
                    .otherwise("UNCHANGED")
                )
            
            # Add previous value columns for changed records
            for col_name in scd_columns:
                scd3_df = scd3_df.withColumn(
                    f"{col_name}_previous",
                    when(col("record_status") == "UPDATED", col(f"target.{col_name}"))
                    .otherwise(None)
                )
            
            # Filter only changed records
            changed_records = scd3_df.filter(
                col("record_status").isin(["NEW", "UPDATED"])
            )
            
            # Record performance metrics
            self.performance_metrics['scd3_sync_time'] = time.time() - start_time
            
            return changed_records
            
        except Exception as e:
            logger.error(f"Lazy SCD3 sync failed: {e}")
            raise
    
    def chunked_processing(self, df: DataFrame, operation_func, 
                          chunk_size: int = None) -> Iterator[DataFrame]:
        """Process DataFrame in chunks for memory efficiency"""
        try:
            if chunk_size is None:
                chunk_size = self.chunk_size
            
            # Get total record count
            total_records = df.count()
            logger.info(f"Processing {total_records:,} records in chunks of {chunk_size:,}")
            
            # Process in chunks
            offset = 0
            while offset < total_records:
                # Get chunk
                chunk_df = df.limit(chunk_size).offset(offset)
                
                # Apply operation
                result_chunk = operation_func(chunk_df)
                
                yield result_chunk
                
                offset += chunk_size
                
                # Memory cleanup
                chunk_df.unpersist()
                
        except Exception as e:
            logger.error(f"Chunked processing failed: {e}")
            raise
    
    def stream_to_database(self, df: DataFrame, target_config: Dict, 
                          table_name: str, mode: str = "append") -> None:
        """Stream DataFrame to database with lazy evaluation"""
        try:
            # Build connection string
            connection_string = self._build_connection_string(target_config)
            
            # Stream to database
            df.write \
                .format("jdbc") \
                .option("url", connection_string) \
                .option("dbtable", table_name) \
                .option("driver", self._get_driver(target_config)) \
                .mode(mode) \
                .save()
                
        except Exception as e:
            logger.error(f"Failed to stream to database: {e}")
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for large dataset processing"""
        return {
            **self.performance_metrics,
            "memory_usage": psutil.virtual_memory().percent,
            "available_memory": psutil.virtual_memory().available,
            "chunk_size": self.chunk_size
        }
    
    def _build_connection_string(self, config: Dict) -> str:
        """Build database connection string for Azure SQL"""
        if config.get('database_type') == 'sqlserver':
            # Use JDBC URL from config if available, otherwise build it
            return config.get('jdbc_url', f"jdbc:sqlserver://{config['host']}:{config['port']};databaseName={config['database']};encrypt=true;trustServerCertificate=false;loginTimeout=30;")
        elif config.get('database_type') == 'mysql':
            return f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}"
        elif config.get('database_type') == 'postgresql':
            return f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
        elif config.get('database_type') == 'oracle':
            return f"jdbc:oracle:thin:@{config['host']}:{config['port']}:{config['database']}"
        else:
            # Default to Azure SQL
            return config.get('jdbc_url', f"jdbc:sqlserver://{config['host']}:{config['port']};databaseName={config['database']};encrypt=true;trustServerCertificate=false;loginTimeout=30;")
    
    def _get_driver(self, config: Dict) -> str:
        """Get appropriate JDBC driver for Azure SQL"""
        if config.get('database_type') == 'sqlserver':
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        elif config.get('database_type') == 'mysql':
            return "com.mysql.cj.jdbc.Driver"
        elif config.get('database_type') == 'postgresql':
            return "org.postgresql.Driver"
        elif config.get('database_type') == 'oracle':
            return "oracle.jdbc.driver.OracleDriver"
        else:
            # Default to Azure SQL
            return "com.microsoft.sqlserver.jdbc.SQLServerDriver"
