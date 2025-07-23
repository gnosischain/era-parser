"""Fixed ClickHouse exporter that properly loads all data types including sync_aggregates"""

import json
import logging
import tempfile
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
from .base import BaseExporter
from .clickhouse_service import ClickHouseService

logger = logging.getLogger(__name__)

class ClickHouseExporter(BaseExporter):
    """Fixed ClickHouse exporter that loads all data types into separate tables"""

    def __init__(self, era_info: Dict[str, Any], era_file_path: str = None):
        """
        Initialize ClickHouse exporter
        
        Args:
            era_info: Era information dictionary
            era_file_path: Path to era file (for hash calculation)
        """
        super().__init__(era_info)
        self.era_file_path = era_file_path
        self.service = ClickHouseService()
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
        
        # Calculate file hash for tracking
        if era_file_path:
            self.file_hash = ClickHouseService.calculate_file_hash(era_file_path)
        else:
            self.file_hash = f"remote_{self.era_number}"

    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export complete blocks to ClickHouse - not used in new approach"""
        # This is only called for single block exports, which we redirect to load_data_to_table
        self.load_data_to_table(blocks, "blocks")

    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to ClickHouse - not used in new approach"""
        # This is only called for single data type exports, which we redirect to load_data_to_table
        self.load_data_to_table(data, data_type)

    def load_all_data_types(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """Load all data types into their respective ClickHouse tables"""
        if not all_data:
            logger.warning("No data to load")
            return

        # Check if era already processed
        if self.service.is_era_processed(self.network, self.era_number, self.file_hash):
            logger.info(f"Era {self.era_number} already processed, skipping")
            return

        try:
            # Mark as processing
            self.service.mark_era_processing(self.network, self.era_number, self.file_hash)
            logger.info(f"Processing era {self.era_number} - loading all data types")
            
            total_records = 0
            loaded_tables = []
            
            # Load each data type into its table
            for data_type, data_list in all_data.items():
                if data_list:  # Only load non-empty data
                    logger.info(f"Loading {len(data_list)} records into {data_type} table")
                    records_loaded = self.load_data_to_table(data_list, data_type)
                    total_records += records_loaded
                    loaded_tables.append(f"{data_type}({records_loaded})")
                else:
                    logger.info(f"No data for {data_type}, skipping")
            
            # Mark as successful
            self.service.mark_era_success(self.network, self.era_number, self.file_hash, total_records)
            logger.info(f"Successfully loaded era {self.era_number}: {', '.join(loaded_tables)} - Total: {total_records} records")

        except Exception as e:
            logger.error(f"Failed to load era {self.era_number}: {e}")
            self.service.mark_era_failed(self.network, self.era_number, self.file_hash, str(e))
            raise

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