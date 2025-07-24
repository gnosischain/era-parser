"""Optimized ClickHouse exporter with direct data preparation and bulk loading"""

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
    """Optimized ClickHouse exporter with direct data preparation and bulk loading"""

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
        Optimized loading of all data types with minimal DataFrame overhead
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
        OPTIMIZATION: Direct preparation of data without DataFrame intermediate step
        """
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
            'request_index', 'deposit_request_index', 'records_inserted', 'participating_validators'
        }
        
        datetime_columns = {'timestamp_utc'}
        
        bulk_data = []
        for row in data_list:
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
                    # Handle timestamp_utc conversion
                    converted_dt = self._convert_to_datetime(value)
                    row_data.append(converted_dt)
                else:
                    # Fast string conversion
                    if value is None:
                        row_data.append('')
                    else:
                        row_data.append(str(value))
            
            bulk_data.append(row_data)
        
        return bulk_data

    def _convert_to_datetime(self, value):
        """Robust datetime conversion for ClickHouse DateTime columns"""
        from datetime import datetime
        import pandas as pd
        
        # Handle None, NaN, empty string cases
        if pd.isna(value) if hasattr(pd, 'isna') else value is None or value == '':
            return datetime(1970, 1, 1)
        
        # Handle string values
        if isinstance(value, str):
            # Handle empty or default datetime strings
            if value in ['1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00']:
                return datetime(1970, 1, 1)
            
            try:
                # Parse ISO datetime string
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
                    
                    return datetime.fromisoformat(dt_str)
                else:
                    # Try to parse as Unix timestamp string
                    try:
                        timestamp = float(value)
                        if timestamp > 0 and timestamp < 2147483647:  # Valid Unix timestamp range
                            return datetime.fromtimestamp(timestamp)
                        else:
                            return datetime(1970, 1, 1)
                    except (ValueError, TypeError):
                        return datetime(1970, 1, 1)
            except (ValueError, TypeError):
                return datetime(1970, 1, 1)
        
        # Handle numeric values (Unix timestamps)
        elif isinstance(value, (int, float)):
            try:
                if value > 0 and value < 2147483647:  # Valid Unix timestamp range
                    return datetime.fromtimestamp(value)
                else:
                    return datetime(1970, 1, 1)
            except (ValueError, TypeError, OSError):
                return datetime(1970, 1, 1)
        
        # Handle datetime objects (should already be correct)
        elif hasattr(value, 'timestamp'):  # datetime-like object
            return value
        
        # Fallback for any other type
        else:
            return datetime(1970, 1, 1)

    def _direct_bulk_insert(self, bulk_data: List[List], table_name: str) -> int:
        """
        OPTIMIZATION: Direct bulk insert bypassing DataFrame creation entirely
        """
        if not bulk_data:
            logger.info(f"No data to load into {table_name}")
            return 0

        try:
            expected_columns = self.service._get_table_columns(table_name)
            
            # OPTIMIZATION 2: Use optimized batch sizes based on data volume
            total_records = len(bulk_data)
            
            if total_records <= 10000:
                # Small datasets: single insert
                self.service.client.insert(table_name, bulk_data, column_names=expected_columns)
                logger.info(f"Direct bulk inserted {total_records} records into {table_name}")
                return total_records
            else:
                # Large datasets: use optimized streaming
                return self._optimized_streaming_insert(bulk_data, table_name, expected_columns)
            
        except Exception as e:
            logger.error(f"Failed to bulk insert into {table_name}: {e}")
            raise

    def _optimized_streaming_insert(self, bulk_data: List[List], table_name: str, 
                                  expected_columns: List[str]) -> int:
        """
        OPTIMIZATION: Use larger batch sizes and optimized streaming for maximum throughput
        """
        # OPTIMIZATION 3: Much larger batch sizes for better throughput
        batch_size = 100000  # Increased from 100 to 100,000 for maximum throughput
        total_inserted = 0
        
        logger.info(f"Optimized streaming insert {len(bulk_data)} records into {table_name} with batch size {batch_size}")
        
        for start_idx in range(0, len(bulk_data), batch_size):
            batch = bulk_data[start_idx:start_idx + batch_size]
            
            # Direct insert with no additional processing
            self.service.client.insert(table_name, batch, column_names=expected_columns)
            total_inserted += len(batch)
            
            # Progress logging for very large datasets
            if len(bulk_data) > 500000 and start_idx % (batch_size * 5) == 0:  # Every 5 batches for large datasets
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