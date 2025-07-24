"""ClickHouse exporter with granular era state management"""

import json
import logging
import tempfile
import time
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
from .base import BaseExporter
from .clickhouse_service import ClickHouseService
from .era_state_manager import EraStateManager

logger = logging.getLogger(__name__)

class ClickHouseExporter(BaseExporter):
    """ClickHouse exporter with granular dataset state tracking"""

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
        Load all data types into their respective ClickHouse tables with granular state tracking.
        Only processes datasets that haven't been completed yet.
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
        
        # Process each pending dataset independently
        for dataset in pending_datasets:
            if dataset not in all_data:
                logger.warning(f"Dataset {dataset} not found in provided data, skipping")
                continue
                
            data_list = all_data[dataset]
            if not data_list:
                logger.info(f"No data for {dataset}, marking as completed with 0 rows")
                self.state_manager.complete_dataset(self.era_filename, dataset, 0)
                continue
            
            # Claim dataset for processing
            if not self.state_manager.claim_dataset(self.era_filename, dataset, self.worker_id, self.file_hash):
                logger.info(f"Dataset {dataset} already being processed by another worker, skipping")
                continue
            
            # Process the dataset
            start_time = time.time()
            try:
                logger.info(f"Loading {len(data_list)} records into {dataset} table")
                records_loaded = self.load_data_to_table(data_list, dataset)
                
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

    def load_data_to_table(self, data: List[Dict[str, Any]], table_name: str) -> int:
        """Load data into specific ClickHouse table"""
        if not data:
            logger.info(f"No data to load into {table_name}")
            return 0

        try:
            # Create DataFrame and load directly
            df = pd.DataFrame(data)
            
            if df.empty:
                logger.warning(f"Empty DataFrame for {table_name}")
                return 0

            # Get expected columns for this table
            expected_columns = self.service._get_table_columns(table_name)
            
            # Create aligned DataFrame with proper columns
            aligned_df = pd.DataFrame()
            
            for col in expected_columns:
                if col in df.columns:
                    aligned_df[col] = df[col]
                else:
                    # Provide default values for missing columns
                    if col in ['block_hash', 'fee_recipient', 'signature', 'version', 'timestamp_utc', 
                              'parent_root', 'state_root', 'beacon_block_root', 'transaction_hash', 
                              'address', 'error_message', 'status', 'randao_reveal', 'graffiti',
                              'eth1_deposit_root', 'eth1_block_hash', 'aggregation_bits',
                              'source_root', 'target_root', 'pubkey', 'withdrawal_credentials',
                              'sync_committee_bits', 'sync_committee_signature', 'proof',
                              'from_bls_pubkey', 'to_execution_address', 'commitment',
                              'source_address', 'validator_pubkey', 'source_pubkey', 'target_pubkey',
                              'header_1_parent_root', 'header_1_state_root', 'header_1_body_root', 
                              'header_1_signature', 'header_2_parent_root', 'header_2_state_root', 
                              'header_2_body_root', 'header_2_signature', 'att_1_beacon_block_root',
                              'att_1_signature', 'att_2_beacon_block_root', 'att_2_signature',
                              'base_fee_per_gas', 'timestamp', 'request_type']:
                        aligned_df[col] = ''
                    else:
                        aligned_df[col] = 0
            
            # Fill NaN values
            for col in aligned_df.columns:
                if aligned_df[col].dtype == 'object':  # String columns
                    aligned_df[col] = aligned_df[col].fillna('')
                else:  # Numeric columns
                    aligned_df[col] = aligned_df[col].fillna(0)
            
            # Load data using service method
            records_loaded = self.service.load_dataframe_to_table(aligned_df, table_name)
            return records_loaded
            
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