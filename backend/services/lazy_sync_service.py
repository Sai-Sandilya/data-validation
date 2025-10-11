"""
PySpark Lazy Sync Service for 1B+ Records
Handles massive datasets with chunked processing and memory optimization using PySpark
"""

from typing import Dict, List, Any, Optional, Iterator
from core.spark_lazy_evaluation import SparkLazyEvaluator
from core.spark_session import get_spark
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)

class LazySyncService:
    """PySpark service for handling 1B+ records with lazy evaluation"""
    
    def __init__(self):
        self.spark = get_spark()
        if not self.spark:
            logger.warning("PySpark not available, using fallback mode with direct database connections")
            self.lazy_evaluator = None
            self.sync_stats = {
                'total_records_processed': 0,
                'chunks_processed': 0,
                'start_time': None,
                'end_time': None,
                'memory_usage': [],
                'pyspark_enabled': False,
                'lazy_evaluation_enabled': False
            }
        else:
            self.lazy_evaluator = SparkLazyEvaluator(self.spark)
            self.sync_stats = {
                'total_records_processed': 0,
                'chunks_processed': 0,
                'start_time': None,
                'end_time': None,
                'memory_usage': [],
                'pyspark_enabled': True,
                'lazy_evaluation_enabled': True
            }
        
        logger.info(f"LazySyncService initialized - PySpark: {self.spark is not None}")
    
    def lazy_compare_tables(self, source_config: Dict, target_config: Dict, 
                           table_name: str, business_keys: List[str], 
                           filters: Dict = None) -> Dict[str, Any]:
        """Compare tables with PySpark lazy evaluation for large datasets"""
        try:
            start_time = time.time()
            self.sync_stats['start_time'] = datetime.now()
            
            logger.info(f"Starting PySpark lazy comparison for table: {table_name}")
            
            # Create PySpark lazy DataFrames
            source_df, target_df = self.lazy_evaluator.create_lazy_dataframe(
                source_config, target_config, table_name, filters
            )
            
            # Get record counts using PySpark
            source_count = source_df.count()
            target_count = target_df.count()
            
            logger.info(f"PySpark - Source records: {source_count:,}, Target records: {target_count:,}")
            
            # Perform PySpark lazy hash comparison
            comparison_df = self.lazy_evaluator.lazy_hash_comparison(
                source_df, target_df, business_keys
            )
            
            # Get comparison results using PySpark
            results = self._get_pyspark_comparison_results(comparison_df)
            
            # Update stats
            self.sync_stats['total_records_processed'] = max(source_count, target_count)
            self.sync_stats['end_time'] = datetime.now()
            
            # Add PySpark performance metrics
            results['performance'] = self.lazy_evaluator.get_performance_metrics()
            results['processing_time'] = time.time() - start_time
            results['pyspark_enabled'] = True
            results['lazy_evaluation'] = True
            
            logger.info(f"PySpark lazy comparison completed for {table_name} in {results['processing_time']:.2f}s")
            return results
            
        except Exception as e:
            logger.error(f"PySpark lazy table comparison failed: {e}")
            raise
    
    def lazy_scd_sync(self, source_config: Dict, target_config: Dict, 
                     table_name: str, business_keys: List[str], 
                     scd_type: str, scd_columns: List[str] = None,
                     filters: Dict = None) -> Dict[str, Any]:
        """Perform SCD sync with lazy evaluation for large datasets"""
        try:
            start_time = time.time()
            self.sync_stats['start_time'] = datetime.now()
            
            logger.info(f"Starting lazy SCD {scd_type} sync for table: {table_name}")
            
            # Create lazy DataFrames
            source_df, target_df = self.lazy_evaluator.create_lazy_dataframe(
                source_config, target_config, table_name, filters
            )
            
            # Get source record count
            source_count = source_df.count()
            logger.info(f"Processing {source_count:,} source records")
            
            # Calculate optimal chunk size
            optimal_chunk_size = self.lazy_evaluator.get_optimal_chunk_size(source_count)
            logger.info(f"Using chunk size: {optimal_chunk_size:,}")
            
            # Process in chunks
            sync_results = []
            chunk_count = 0
            
            for chunk_df in self.lazy_evaluator.chunked_processing(
                source_df, 
                lambda df: self._process_scd_chunk(df, target_df, business_keys, scd_type, scd_columns),
                optimal_chunk_size
            ):
                chunk_count += 1
                chunk_results = chunk_df.collect()
                sync_results.extend(chunk_results)
                
                # Log progress
                if chunk_count % 10 == 0:
                    logger.info(f"Processed {chunk_count} chunks, {len(sync_results):,} records")
                
                # Memory cleanup
                chunk_df.unpersist()
            
            # Update stats
            self.sync_stats['total_records_processed'] = source_count
            self.sync_stats['chunks_processed'] = chunk_count
            self.sync_stats['end_time'] = datetime.now()
            
            return {
                'success': True,
                'records_processed': len(sync_results),
                'chunks_processed': chunk_count,
                'scd_type': scd_type,
                'processing_time': time.time() - start_time,
                'performance': self.lazy_evaluator.get_performance_metrics()
            }
            
        except Exception as e:
            logger.error(f"Lazy SCD sync failed: {e}")
            raise
    
    def _process_scd_chunk(self, source_chunk: Any, target_df: Any, 
                          business_keys: List[str], scd_type: str, 
                          scd_columns: List[str] = None) -> Any:
        """Process a single chunk for SCD operations"""
        try:
            if scd_type == 'SCD2':
                return self.lazy_evaluator.lazy_scd2_sync(
                    source_chunk, target_df, business_keys, scd_columns or []
                )
            elif scd_type == 'SCD3':
                return self.lazy_evaluator.lazy_scd3_sync(
                    source_chunk, target_df, business_keys, scd_columns or []
                )
            else:
                raise ValueError(f"Unsupported SCD type: {scd_type}")
                
        except Exception as e:
            logger.error(f"SCD chunk processing failed: {e}")
            raise
    
    def _get_pyspark_comparison_results(self, comparison_df: Any) -> Dict[str, Any]:
        """Get comparison results from PySpark DataFrame"""
        try:
            from pyspark.sql.functions import col
            
            # Count different types of mismatches using PySpark
            match_count = comparison_df.filter(col("hash_match") == "MATCH").count()
            missing_source = comparison_df.filter(col("hash_match") == "MISSING_IN_SOURCE").count()
            missing_target = comparison_df.filter(col("hash_match") == "MISSING_IN_TARGET").count()
            mismatch_count = comparison_df.filter(col("hash_match") == "MISMATCH").count()
            
            total_records = match_count + missing_source + missing_target + mismatch_count
            
            return {
                'total_records': total_records,
                'matches': match_count,
                'missing_in_source': missing_source,
                'missing_in_target': missing_target,
                'mismatches': mismatch_count,
                'match_percentage': (match_count / total_records) * 100 if total_records > 0 else 0,
                'pyspark_processed': True
            }
            
        except Exception as e:
            logger.error(f"Failed to get PySpark comparison results: {e}")
            return {
                'total_records': 0,
                'matches': 0,
                'missing_in_source': 0,
                'missing_in_target': 0,
                'mismatches': 0,
                'match_percentage': 0,
                'pyspark_processed': False,
                'error': str(e)
            }
    
    def get_sync_statistics(self) -> Dict[str, Any]:
        """Get comprehensive sync statistics"""
        return {
            **self.sync_stats,
            'performance_metrics': self.lazy_evaluator.get_performance_metrics(),
            'spark_metrics': {
                'executor_memory': self.spark.conf.get('spark.executor.memory'),
                'driver_memory': self.spark.conf.get('spark.driver.memory'),
                'max_result_size': self.spark.conf.get('spark.driver.maxResultSize')
            }
        }
    
    def optimize_for_large_datasets(self, total_records: int) -> Dict[str, Any]:
        """Optimize Spark configuration for large datasets"""
        try:
            # Calculate optimal configuration
            optimal_chunk_size = self.lazy_evaluator.get_optimal_chunk_size(total_records)
            
            # Update Spark configuration
            self.spark.conf.set('spark.sql.adaptive.enabled', 'true')
            self.spark.conf.set('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            self.spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')
            
            # Set memory configuration
            if total_records > 100_000_000:  # 100M+ records
                self.spark.conf.set('spark.sql.adaptive.advisoryPartitionSizeInBytes', '128MB')
                self.spark.conf.set('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes', '256MB')
            
            return {
                'optimal_chunk_size': optimal_chunk_size,
                'total_records': total_records,
                'estimated_chunks': total_records // optimal_chunk_size,
                'configuration_applied': True
            }
            
        except Exception as e:
            logger.error(f"Failed to optimize for large datasets: {e}")
            return {
                'optimal_chunk_size': self.lazy_evaluator.chunk_size,
                'total_records': total_records,
                'estimated_chunks': total_records // self.lazy_evaluator.chunk_size,
                'configuration_applied': False,
                'error': str(e)
            }
