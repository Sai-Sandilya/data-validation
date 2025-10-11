import os
import logging
from config.db_config import PYSPARK_CONFIG

logger = logging.getLogger(__name__)

def get_spark():
    """
    Get Spark session for Azure Databricks or local development
    """
    try:
        import findspark
        findspark.init()
        
        from pyspark.sql import SparkSession
        
        # Check if we're running in Databricks
        try:
            # In Databricks, SparkSession is already available
            spark = SparkSession.builder.getOrCreate()
            logger.info("PySpark session created successfully in Azure Databricks")
        except:
            # For local development, create new session
            spark = SparkSession.builder \
                .appName("DataValidationSync-PySpark") \
                .master("local[*]") \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .getOrCreate()
            logger.info("PySpark session created successfully in local mode")
        
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"Spark version: {spark.version}")
        
        return spark
        
    except ImportError as e:
        logger.warning(f"PySpark not available: {e}")
        return None
    except Exception as e:
        logger.warning(f"Error creating Spark session: {e}")
        return None

def get_spark_context():
    """Get Spark context for low-level operations"""
    try:
        spark = get_spark()
        if spark:
            return spark.sparkContext
        return None
    except Exception as e:
        logger.error(f"Error getting Spark context: {e}")
        return None

def stop_spark():
    """Stop Spark session"""
    try:
        spark = get_spark()
        if spark:
            spark.stop()
            logger.info("Spark session stopped")
    except Exception as e:
        logger.error(f"Error stopping Spark session: {e}")