"""Optimized ClickHouse exporter with direct data preparation and single timestamp"""

import json
import logging
import tempfile
import time
from typing import List, Dict, Any

import pandas as pd
from .base import BaseExporter
from .clickhouse_service import ClickHouseService
from .era_state_manager import EraStateManager

logger = logging.getLogger(__name__)

class ClickHouseExporter(BaseExporter):
    """Optimized ClickHouse exporter with direct data preparation and single timestamp"""

    def __init__(self, era_info: Dict[str, Any], era_file_path: str = None):
        """
        Initialize ClickHouse exporter with state management
        
        Args:
            era_info: Era information dictionary
            era_file_path: Path to era file (for hash calculation)
        """
        super().__init__(era_info)
        self.era_file_path = era_file_path
        self.service = ClickHouseService()
        self.state_manager = EraStateManager()
        self.network = era_info.get('network', 'mainnet')
        
        # Handle era_number properly
        self.era_number = era_info.get('era_number')
        if self.era_number is None and era_file_path:
            import os
            filename = os.path.basename(era_file_path)
            parts = filename.replace('.era', '').split('-')
            if len(parts) >= 2:
                try:
                    self.era_number = int(parts[-2])
                except (ValueError, IndexError):
                    self.era_number = 0
            else:
                self.era_number = 0
        elif self.era_number is None:
            self.era_number = 0
        
        # Get era filename and file hash
        if era_file_path:
            self.era_filename = self.state_manager.get_era_filename_from_path(era_file_path)
            self.file_hash = EraStateManager.calculate_file_hash(era_file_path)
        else:
            self.era_filename = f"remote_{self.era_number}"
            self.file_hash = f"remote_{self.era_number}"
        
        # Generate worker ID
        import uuid
        self.worker_id = str(uuid.uuid4())[:8]

    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export complete blocks to ClickHouse - not used in new approach"""
        # This is only called for single block exports, which we redirect to load_data_to_table
        self.load_data_to_table(blocks, "blocks")

    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to ClickHouse - not used in new approach"""
        # This is only called for single data type exports, which we redirect to load_data_to_table
        self.load_data_to_table(data, data_type)

    def load_all_data_types(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """
        Optimized loading of all data types with minimal DataFrame overhead and single timestamp
        """
        if not all_data:
            logger.warning("No data to load")
            return

        # Get list of datasets that need processing
        target_datasets = list(all_data.keys())
        pending_datasets = self.state_manager.get_pending_datasets(self.era_filename, target_datasets)
        
        if not pending_datasets:
            logger.info(f"Era {self.era_filename} already fully processed, skipping")
            return
        
        logger.info(f"Processing era {self.era_filename} - {len(pending_datasets)} datasets need work: {pending_datasets}")
        
        # OPTIMIZATION 1: Prepare all data at once instead of per-dataset
        prepared_data = self._prepare_all_datasets_bulk(all_data, pending_datasets)
        
        # Process each pending dataset independently
        for dataset in pending_datasets:
            if dataset not in prepared_data:
                logger.info(f"No data for {dataset}, marking as completed with 0 rows")
                self.state_manager.complete_dataset(self.era_filename, dataset, 0)
                continue
            
            # Claim dataset for processing
            if not self.state_manager.claim_dataset(self.era_filename, dataset, self.worker_id, self.file_hash):
                logger.info(f"Dataset {dataset} already being processed by another worker, skipping")
                continue
            
            # Process the dataset with pre-prepared data
            start_time = time.time()
            try:
                bulk_data = prepared_data[dataset]
                records_loaded = self._direct_bulk_insert(bulk_data, dataset)
                
                # Calculate processing time
                processing_duration_ms = int((time.time() - start_time) * 1000)
                
                # Mark as completed
                self.state_manager.complete_dataset(
                    self.era_filename, 
                    dataset, 
                    records_loaded, 
                    processing_duration_ms
                )
                
                logger.info(f"Successfully completed {dataset}: {records_loaded} records in {processing_duration_ms}ms")
                
            except Exception as e:
                # Mark as failed
                error_msg = f"Failed to load {dataset}: {str(e)}"
                self.state_manager.fail_dataset(self.era_filename, dataset, error_msg)
                logger.error(error_msg)
                # Continue with other datasets instead of failing completely
                continue
        
        # Log final status
        remaining_pending = self.state_manager.get_pending_datasets(self.era_filename, target_datasets)
        if not remaining_pending:
            logger.info(f"Era {self.era_filename} now fully processed!")
        else:
            logger.warning(f"Era {self.era_filename} still has {len(remaining_pending)} pending datasets: {remaining_pending}")

    def _prepare_all_datasets_bulk(self, all_data: Dict[str, List[Dict[str, Any]]], 
                                 pending_datasets: List[str]) -> Dict[str, List[List]]:
        """
        OPTIMIZATION: Prepare all dataset data in bulk to avoid repeated DataFrame operations
        """
        prepared_data = {}
        
        for dataset in pending_datasets:
            if dataset not in all_data or not all_data[dataset]:
                continue
                
            data_list = all_data[dataset]
            logger.info(f"Preparing {len(data_list)} records for {dataset}")
            
            # Get expected columns for this table
            expected_columns = self.service._get_table_columns(dataset)
            
            # OPTIMIZATION: Direct conversion to bulk format without DataFrame
            bulk_data = self._prepare_dataset_direct(data_list, expected_columns)
            prepared_data[dataset] = bulk_data
            
        return prepared_data

    def _prepare_dataset_direct(self, data_list: List[Dict[str, Any]], 
                              expected_columns: List[str]) -> List[List]:
        """
        SIMPLIFIED: Direct preparation of data with SINGLE timestamp logic
        """
        from datetime import datetime
        
        # Pre-identify numeric columns for faster processing
        numeric_columns = {
            'slot', 'era_number', 'block_number', 'proposer_index', 'gas_used', 
            'gas_limit', 'withdrawal_index', 'validator_index', 'amount', 
            'attestation_index', 'committee_index', 'source_epoch', 'target_epoch',
            'deposit_index', 'exit_index', 'epoch', 'transaction_index',
            'attestation_slot', 'eth1_deposit_count', 'blob_gas_used', 'excess_blob_gas',
            'slashing_index', 'header_1_slot', 'header_1_proposer_index',
            'header_2_slot', 'header_2_proposer_index', 'att_1_slot', 'att_1_committee_index',
            'att_1_source_epoch', 'att_1_target_epoch', 'att_2_slot', 'att_2_committee_index',
            'att_2_source_epoch', 'att_2_target_epoch', 'change_index', 'commitment_index',
            'request_index', 'deposit_request_index', 'records_inserted', 'participating_validators',
            'transactions_count', 'withdrawals_count'
        }
        
        # SIMPLIFIED: Only timestamp_utc is a datetime column - that's it!
        datetime_columns = {'timestamp_utc'}
        
        # SAFE FALLBACK - never return None
        SAFE_FALLBACK = datetime(1970, 1, 2)
        
        bulk_data = []
        for row_idx, row in enumerate(data_list):
            row_data = []
            for col in expected_columns:
                value = row.get(col)
                
                if col in numeric_columns:
                    # Fast numeric conversion
                    if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                        row_data.append(0)
                    else:
                        try:
                            # Direct conversion for common cases
                            if isinstance(value, int):
                                row_data.append(value)
                            elif isinstance(value, float):
                                row_data.append(int(value))
                            else:
                                row_data.append(int(float(str(value))))
                        except (ValueError, TypeError):
                            row_data.append(0)
                            
                elif col in datetime_columns:
                    # DateTime conversion - ONLY for timestamp_utc
                    converted_dt = self._convert_to_datetime(value)
                    
                    # Double-check: if somehow _convert_to_datetime returns None, use fallback
                    if converted_dt is None:
                        logger.error(f"Row {row_idx}, Column {col}: _convert_to_datetime returned None! Using fallback. Original value: {value}")
                        converted_dt = SAFE_FALLBACK
                    
                    # Triple-check: ensure it's actually a datetime object
                    if not isinstance(converted_dt, datetime):
                        logger.error(f"Row {row_idx}, Column {col}: Expected datetime but got {type(converted_dt)}! Using fallback. Value: {converted_dt}")
                        converted_dt = SAFE_FALLBACK
                    
                    row_data.append(converted_dt)
                    
                else:
                    # Everything else is a string - SIMPLE!
                    if value is None:
                        row_data.append('')
                    else:
                        row_data.append(str(value))
            
            bulk_data.append(row_data)
        
        return bulk_data

    def _convert_to_datetime(self, value):
        """Robust datetime conversion for ClickHouse DateTime columns with proper range validation"""
        from datetime import datetime
        import pandas as pd
        
        # ClickHouse DateTime range: [1970-01-01 00:00:00, 2106-02-07 06:28:15]
        # Use a safe fallback that's well within the range
        SAFE_FALLBACK = datetime(1970, 1, 2)  # One day after minimum to be safe
        
        # Handle None, NaN, empty string cases
        if value is None or value == '':
            return SAFE_FALLBACK
        
        # Handle pandas NaN
        if hasattr(pd, 'isna') and pd.isna(value):
            return SAFE_FALLBACK
        
        # Handle string values
        if isinstance(value, str):
            # Handle empty or default datetime strings
            if value in ['1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00', '0', 'null', 'NULL']:
                return SAFE_FALLBACK
            
            try:
                # First try to parse as Unix timestamp string (common case for execution payload timestamp)
                try:
                    timestamp = int(value)
                    # Validate timestamp range: 0 to 4294967295 (max 32-bit unsigned int)
                    # ClickHouse DateTime max is around 4294944000 (2106-02-07)
                    if 0 < timestamp < 4294944000:  # Valid Unix timestamp range for ClickHouse
                        return datetime.fromtimestamp(timestamp)
                    elif timestamp == 0:
                        return SAFE_FALLBACK
                    else:
                        # Timestamp out of range
                        logger.warning(f"Timestamp {timestamp} out of ClickHouse DateTime range, using fallback")
                        return SAFE_FALLBACK
                except (ValueError, TypeError):
                    pass
                
                # Then try ISO datetime string parsing
                if 'T' in value:
                    # Clean up timezone info for fromisoformat
                    dt_str = value.replace('Z', '')
                    if '+' in dt_str:
                        dt_str = dt_str.split('+')[0]
                    if dt_str.endswith('Z'):
                        dt_str = dt_str[:-1]
                    
                    # Handle microseconds if present
                    if '.' in dt_str:
                        dt_str = dt_str.split('.')[0]  # Remove microseconds
                    
                    parsed_dt = datetime.fromisoformat(dt_str)
                    
                    # Validate parsed datetime is within ClickHouse range
                    if datetime(1970, 1, 1) <= parsed_dt <= datetime(2106, 2, 7):
                        return parsed_dt
                    else:
                        logger.warning(f"Parsed datetime {parsed_dt} out of ClickHouse range, using fallback")
                        return SAFE_FALLBACK
                else:
                    # Try to parse as float timestamp string
                    try:
                        timestamp = float(value)
                        if 0 < timestamp < 4294944000:  # Valid Unix timestamp range
                            return datetime.fromtimestamp(timestamp)
                        else:
                            return SAFE_FALLBACK
                    except (ValueError, TypeError):
                        return SAFE_FALLBACK
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse datetime string '{value}': {e}")
                return SAFE_FALLBACK
        
        # Handle numeric values (Unix timestamps)
        elif isinstance(value, (int, float)):
            try:
                # Validate timestamp range
                if 0 < value < 4294944000:  # Valid Unix timestamp range for ClickHouse
                    return datetime.fromtimestamp(value)
                else:
                    logger.warning(f"Numeric timestamp {value} out of ClickHouse range, using fallback")
                    return SAFE_FALLBACK
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Failed to convert timestamp {value}: {e}")
                return SAFE_FALLBACK
        
        # Handle datetime objects (should already be correct)
        elif isinstance(value, datetime):
            # Validate datetime is within ClickHouse range
            if datetime(1970, 1, 1) <= value <= datetime(2106, 2, 7):
                return value
            else:
                logger.warning(f"Datetime object {value} out of ClickHouse range, using fallback")
                return SAFE_FALLBACK
        
        # Fallback for any other type
        else:
            logger.warning(f"Unexpected datetime value type: {type(value)} = {value}")
            return SAFE_FALLBACK

    def _validate_datetime_column(self, bulk_data: List[List], expected_columns: List[str]) -> List[List]:
        """Validate datetime columns to ensure no None values - SIMPLIFIED single column"""
        from datetime import datetime
        
        datetime_columns = {'timestamp_utc'}  # ONLY timestamp_utc
        SAFE_FALLBACK = datetime(1970, 1, 2)
        
        # Find datetime column indices
        datetime_indices = []
        for i, col in enumerate(expected_columns):
            if col in datetime_columns:
                datetime_indices.append(i)
        
        if not datetime_indices:
            return bulk_data  # No datetime columns to validate
        
        # Validate and fix datetime values
        fixed_data = []
        for row in bulk_data:
            fixed_row = list(row)  # Make a copy
            
            for col_idx in datetime_indices:
                if col_idx < len(fixed_row):
                    value = fixed_row[col_idx]
                    
                    # Check for None or invalid values
                    if value is None:
                        logger.warning(f"Found None value in datetime column {expected_columns[col_idx]}, using fallback")
                        fixed_row[col_idx] = SAFE_FALLBACK
                    elif not isinstance(value, datetime):
                        logger.warning(f"Found non-datetime value {type(value)} in datetime column {expected_columns[col_idx]}, converting")
                        fixed_row[col_idx] = self._convert_to_datetime(value)
                    elif not (datetime(1970, 1, 1) <= value <= datetime(2106, 2, 7)):
                        logger.warning(f"Found out-of-range datetime {value} in column {expected_columns[col_idx]}, using fallback")
                        fixed_row[col_idx] = SAFE_FALLBACK
            
            fixed_data.append(fixed_row)
        
        return fixed_data

    def _direct_bulk_insert(self, bulk_data: List[List], table_name: str) -> int:
        """
        OPTIMIZATION: Direct bulk insert with ClickHouse Cloud reliability
        """
        if not bulk_data:
            logger.info(f"No data to load into {table_name}")
            return 0

        try:
            expected_columns = self.service._get_table_columns(table_name)
            
            # VALIDATION: Ensure datetime columns are properly formatted
            validated_data = self._validate_datetime_column(bulk_data, expected_columns)
            
            # OPTIMIZATION: Adaptive batch sizing for ClickHouse Cloud
            total_records = len(validated_data)
            
            # Use smaller batches for problem tables, streaming for large datasets
            if table_name == 'attestations' and total_records > 10000:
                return self._cloud_optimized_streaming_insert(validated_data, table_name, expected_columns)
            elif total_records > 15000:
                return self._cloud_optimized_streaming_insert(validated_data, table_name, expected_columns)
            else:
                # Small datasets: direct insert with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.service.client.insert(table_name, validated_data, column_names=expected_columns)
                        logger.info(f"Direct bulk inserted {total_records} records into {table_name}")
                        return total_records
                    except Exception as e:
                        logger.warning(f"Insert attempt {attempt + 1}/{max_retries} failed for {table_name}: {e}")
                        
                        if attempt < max_retries - 1:
                            import time
                            time.sleep(2 ** attempt)
                            
                            # Test and reconnect if needed
                            try:
                                self.service.client.command("SELECT 1")
                            except:
                                logger.info("Reconnecting to ClickHouse...")
                                self.service.client = self.service._connect()
                        else:
                            # Fall back to streaming on final failure
                            logger.warning(f"All attempts failed, using streaming for {table_name}")
                            return self._cloud_optimized_streaming_insert(validated_data, table_name, expected_columns)
            
        except Exception as e:
            logger.error(f"Failed to bulk insert into {table_name}: {e}")
            raise

    def _cloud_optimized_streaming_insert(self, bulk_data: List[List], table_name: str, 
                                        expected_columns: List[str]) -> int:
        """
        OPTIMIZATION: Streaming insert optimized for ClickHouse Cloud with adaptive batch sizes
        """
        # Adaptive batch sizes based on table characteristics
        if table_name == 'attestations':
            batch_size = 3000  # Attestations are complex, use smaller batches
        elif table_name in ['transactions', 'withdrawals']:
            batch_size = 8000  # Medium complexity
        else:
            batch_size = 15000  # Simpler tables can handle larger batches
            
        total_inserted = 0
        
        logger.info(f"Cloud-optimized streaming: {len(bulk_data)} records into {table_name} (batch size: {batch_size})")
        
        for start_idx in range(0, len(bulk_data), batch_size):
            batch = bulk_data[start_idx:start_idx + batch_size]
            batch_num = start_idx // batch_size + 1
            
            # Retry logic for each batch
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.service.client.insert(table_name, batch, column_names=expected_columns)
                    total_inserted += len(batch)
                    break  # Success
                    
                except Exception as e:
                    logger.warning(f"Batch {batch_num} attempt {attempt + 1}/{max_retries} failed: {e}")
                    
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                        
                        # Test connection and reconnect if needed
                        try:
                            self.service.client.command("SELECT 1")
                        except:
                            logger.info("Reconnecting to ClickHouse...")
                            self.service.client = self.service._connect()
                    else:
                        logger.error(f"Batch {batch_num} failed after {max_retries} attempts")
                        raise
            
            # Progress for large datasets
            if len(bulk_data) > 30000 and batch_num % 10 == 0:
                progress = (start_idx + len(batch)) / len(bulk_data) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_inserted:,} records)")
        
        logger.info(f"Successfully streamed {total_inserted:,} records into {table_name}")
        return total_inserted

    def load_data_to_table(self, data: List[Dict[str, Any]], table_name: str) -> int:
        """Optimized loading for single data type with minimal overhead"""
        if not data:
            logger.info(f"No data to load into {table_name}")
            return 0

        try:
            # OPTIMIZATION: Skip DataFrame creation entirely for single table loads
            expected_columns = self.service._get_table_columns(table_name)
            bulk_data = self._prepare_dataset_direct(data, expected_columns)
            return self._direct_bulk_insert(bulk_data, table_name)
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise

    def is_era_processed(self, target_datasets: List[str] = None) -> bool:
        """
        Check if era is already processed for target datasets.
        
        Args:
            target_datasets: Datasets to check (None = all datasets)
            
        Returns:
            True if era is fully processed
        """
        return self.state_manager.is_era_fully_processed(self.era_filename, target_datasets)

    def get_pending_datasets_for_era(self, target_datasets: List[str] = None) -> List[str]:
        """
        Get list of datasets that still need processing for this era.
        
        Args:
            target_datasets: Datasets to check (None = all datasets)
            
        Returns:
            List of pending dataset names
        """
        return self.state_manager.get_pending_datasets(self.era_filename, target_datasets)