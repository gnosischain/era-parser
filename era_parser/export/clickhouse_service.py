"""Optimized ClickHouse service with time-based partitioning and proper ORDER BY keys"""

import os
import logging
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import clickhouse_connect

logger = logging.getLogger(__name__)

class ClickHouseService:
    """Optimized service for beacon chain data with time-based partitioning and bulk loading"""

    def __init__(self):
        """Initialize ClickHouse service from environment variables"""
        self.host = os.getenv('CLICKHOUSE_HOST')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '8443'))
        self.user = os.getenv('CLICKHOUSE_USER', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD')
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'beacon_chain')
        self.secure = os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'

        if not self.host or not self.password:
            raise ValueError("CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD must be set")

        self.client = self._connect()
        self._ensure_tables()

    def _connect(self):
        """Connect to ClickHouse with optimized settings"""
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                secure=self.secure,
                verify=False,
                # Optimization: Increase connection settings for bulk operations
                settings={
                    'max_insert_block_size': 1000000,
                    'insert_quorum': 0,
                    'insert_quorum_timeout': 0,
                    'async_insert': 1,
                    'wait_for_async_insert': 1,
                    'async_insert_max_data_size': 10485760,  # 10MB
                    'async_insert_busy_timeout_ms': 1000,
                }
            )
            client.command("SELECT 1")
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port} with optimized settings")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _ensure_tables(self):
        """Create properly normalized tables with time-based partitioning"""

        # Blocks table - ONLY beacon chain block data with time-based partitioning
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.blocks (
            slot UInt64,
            proposer_index UInt64 DEFAULT 0,
            parent_root String DEFAULT '',
            state_root String DEFAULT '',
            signature String DEFAULT '',
            version String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            randao_reveal String DEFAULT '',
            graffiti String DEFAULT '',
            eth1_deposit_root String DEFAULT '',
            eth1_deposit_count UInt64 DEFAULT 0,
            eth1_block_hash String DEFAULT '',
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, proposer_index)
        """)

        # Sync aggregates table - separate normalized table with optimized ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.sync_aggregates (
            slot UInt64,
            sync_committee_bits String DEFAULT '',
            sync_committee_signature String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            participating_validators UInt32 DEFAULT 0,
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot)
        """)

        # Execution payloads table - separate normalized table
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.execution_payloads (
            slot UInt64,
            parent_hash String DEFAULT '',
            fee_recipient String DEFAULT '',
            state_root String DEFAULT '',
            receipts_root String DEFAULT '',
            logs_bloom String DEFAULT '',
            prev_randao String DEFAULT '',
            block_number UInt64 DEFAULT 0,
            gas_limit UInt64 DEFAULT 0,
            gas_used UInt64 DEFAULT 0,
            timestamp DateTime DEFAULT toDateTime(0),
            timestamp_utc DateTime DEFAULT toDateTime(0),
            base_fee_per_gas String DEFAULT '',
            block_hash String DEFAULT '',
            blob_gas_used UInt64 DEFAULT 0,
            excess_blob_gas UInt64 DEFAULT 0,
            extra_data String DEFAULT '',
            transactions_count UInt64 DEFAULT 0,
            withdrawals_count UInt64 DEFAULT 0,
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, block_number)
        """)

        # Transactions table with optimized ORDER BY for transaction queries
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.transactions (
            slot UInt64,
            block_number UInt64 DEFAULT 0,
            block_hash String DEFAULT '',
            transaction_index UInt64,
            transaction_hash String,
            fee_recipient String DEFAULT '',
            gas_limit UInt64 DEFAULT 0,
            gas_used UInt64 DEFAULT 0,
            base_fee_per_gas String DEFAULT '',
            timestamp String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, transaction_index, transaction_hash)
        """)

        # Withdrawals table with validator-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.withdrawals (
            slot UInt64,
            block_number UInt64 DEFAULT 0,
            block_hash String DEFAULT '',
            withdrawal_index UInt64,
            validator_index UInt64,
            address String,
            amount UInt64,
            timestamp String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, withdrawal_index, validator_index)
        """)

        # Attestations table with committee-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.attestations (
            slot UInt64,
            attestation_index UInt64,
            aggregation_bits String DEFAULT '',
            signature String DEFAULT '',
            attestation_slot UInt64 DEFAULT 0,
            committee_index UInt64 DEFAULT 0,
            beacon_block_root String DEFAULT '',
            source_epoch UInt64 DEFAULT 0,
            source_root String DEFAULT '',
            target_epoch UInt64 DEFAULT 0,
            target_root String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, attestation_index, committee_index)
        """)

        # Deposits table with deposit-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.deposits (
            slot UInt64,
            deposit_index UInt64,
            proof String DEFAULT '',
            pubkey String DEFAULT '',
            withdrawal_credentials String DEFAULT '',
            amount UInt64 DEFAULT 0,
            signature String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, deposit_index, pubkey)
        """)

        # Voluntary exits table with validator-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.voluntary_exits (
            slot UInt64,
            exit_index UInt64,
            signature String DEFAULT '',
            epoch UInt64 DEFAULT 0,
            validator_index UInt64 DEFAULT 0,
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, validator_index, epoch)
        """)

        # Proposer slashings table with slashing-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.proposer_slashings (
            slot UInt64,
            slashing_index UInt64,
            header_1_slot UInt64 DEFAULT 0,
            header_1_proposer_index UInt64 DEFAULT 0,
            header_1_parent_root String DEFAULT '',
            header_1_state_root String DEFAULT '',
            header_1_body_root String DEFAULT '',
            header_1_signature String DEFAULT '',
            header_2_slot UInt64 DEFAULT 0,
            header_2_proposer_index UInt64 DEFAULT 0,
            header_2_parent_root String DEFAULT '',
            header_2_state_root String DEFAULT '',
            header_2_body_root String DEFAULT '',
            header_2_signature String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, slashing_index, header_1_proposer_index)
        """)

        # Attester slashings table with slashing-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.attester_slashings (
            slot UInt64,
            slashing_index UInt64,
            att_1_slot UInt64 DEFAULT 0,
            att_1_committee_index UInt64 DEFAULT 0,
            att_1_beacon_block_root String DEFAULT '',
            att_1_source_epoch UInt64 DEFAULT 0,
            att_1_target_epoch UInt64 DEFAULT 0,
            att_1_signature String DEFAULT '',
            att_2_slot UInt64 DEFAULT 0,
            att_2_committee_index UInt64 DEFAULT 0,
            att_2_beacon_block_root String DEFAULT '',
            att_2_source_epoch UInt64 DEFAULT 0,
            att_2_target_epoch UInt64 DEFAULT 0,
            att_2_signature String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, slashing_index, att_1_committee_index)
        """)

        # BLS changes table with validator-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.bls_changes (
            slot UInt64,
            change_index UInt64,
            signature String DEFAULT '',
            validator_index UInt64 DEFAULT 0,
            from_bls_pubkey String DEFAULT '',
            to_execution_address String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, change_index, validator_index)
        """)

        # Blob commitments table with blob-focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.blob_commitments (
            slot UInt64,
            commitment_index UInt64,
            commitment String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, commitment_index)
        """)

        # Execution requests table with request-type focused ORDER BY
        self.client.command(f"""
        CREATE TABLE IF NOT EXISTS {self.database}.execution_requests (
            slot UInt64,
            request_type String,
            request_index UInt64,
            pubkey String DEFAULT '',
            withdrawal_credentials String DEFAULT '',
            amount UInt64 DEFAULT 0,
            signature String DEFAULT '',
            deposit_request_index UInt64 DEFAULT 0,
            source_address String DEFAULT '',
            validator_pubkey String DEFAULT '',
            source_pubkey String DEFAULT '',
            target_pubkey String DEFAULT '',
            timestamp_utc DateTime DEFAULT toDateTime(0),
            insert_version UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
        ) ENGINE = ReplacingMergeTree(insert_version)
        PARTITION BY toStartOfMonth(timestamp_utc)
        ORDER BY (slot, request_type, request_index)
        """)

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str) -> int:
        """Optimized bulk loading with direct data preparation"""
        try:
            if df.empty:
                logger.warning(f"No data in DataFrame for {table_name}")
                return 0

            # Get expected columns for this table
            expected_columns = self._get_table_columns(table_name)
            
            # OPTIMIZATION 1: Skip DataFrame alignment and prepare data directly
            # Convert to list of lists format that ClickHouse client expects
            bulk_data = self._prepare_bulk_data(df, expected_columns)
            
            if not bulk_data:
                logger.warning(f"No valid data prepared for {table_name}")
                return 0

            # OPTIMIZATION 2: Use single bulk insert instead of batching
            total_records = len(bulk_data)
            
            # For very large datasets, use streaming insert
            if total_records > 100000:
                return self._streaming_bulk_insert(bulk_data, table_name, expected_columns)
            else:
                # Direct bulk insert for smaller datasets
                self.client.insert(table_name, bulk_data, column_names=expected_columns)
                logger.info(f"Bulk inserted {total_records} records into {table_name}")
                return total_records
            
        except Exception as e:
            logger.error(f"Failed to load DataFrame into {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def _prepare_bulk_data(self, df: pd.DataFrame, expected_columns: List[str]) -> List[List]:
        """Efficiently prepare data for bulk insert with robust timestamp handling"""
        bulk_data = []
        
        # OPTIMIZATION: Pre-identify numeric vs string columns to avoid repeated type checking
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
        
        datetime_columns = {'timestamp_utc', 'timestamp'}
        
        # Pre-convert DataFrame to dict for faster row access
        df_dict = df.to_dict('records')
        
        for row in df_dict:
            row_data = []
            for col in expected_columns:
                value = row.get(col)
                
                if col in numeric_columns:
                    # Numeric columns - fast path
                    if pd.isna(value) or value == '' or value is None:
                        row_data.append(0)
                    else:
                        try:
                            # Direct int conversion without float intermediate
                            if isinstance(value, (int, float)):
                                row_data.append(int(value))
                            else:
                                row_data.append(int(float(str(value))))
                        except (ValueError, TypeError):
                            row_data.append(0)
                elif col in datetime_columns:
                    # DateTime columns - robust conversion to ClickHouse DateTime format
                    converted_dt = self._convert_to_datetime(value)
                    row_data.append(converted_dt)
                else:
                    # String columns - fast path
                    if pd.isna(value) or value is None:
                        row_data.append('')
                    else:
                        row_data.append(str(value))
            
            bulk_data.append(row_data)
        
        return bulk_data

    def _convert_to_datetime(self, value) -> datetime:
        """Robust datetime conversion for ClickHouse DateTime columns"""
        # Handle None, NaN, empty string cases
        if pd.isna(value) or value is None or value == '':
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
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to parse datetime string '{value}': {e}")
                return datetime(1970, 1, 1)
        
        # Handle numeric values (Unix timestamps)
        elif isinstance(value, (int, float)):
            try:
                if value > 0 and value < 2147483647:  # Valid Unix timestamp range
                    return datetime.fromtimestamp(value)
                else:
                    return datetime(1970, 1, 1)
            except (ValueError, TypeError, OSError) as e:
                logger.warning(f"Failed to convert timestamp {value}: {e}")
                return datetime(1970, 1, 1)
        
        # Handle datetime objects (should already be correct)
        elif isinstance(value, datetime):
            return value
        
        # Fallback for any other type
        else:
            logger.warning(f"Unexpected datetime value type: {type(value)} = {value}")
            return datetime(1970, 1, 1)

    def _streaming_bulk_insert(self, bulk_data: List[List], table_name: str, expected_columns: List[str]) -> int:
        """Handle very large datasets with streaming inserts"""
        # OPTIMIZATION 3: Use much larger batch sizes for better throughput
        batch_size = 50000  # Increased from 100 to 50,000
        total_inserted = 0
        
        logger.info(f"Streaming insert {len(bulk_data)} records into {table_name} with batch size {batch_size}")
        
        for start_idx in range(0, len(bulk_data), batch_size):
            batch = bulk_data[start_idx:start_idx + batch_size]
            
            # Direct insert without additional processing
            self.client.insert(table_name, batch, column_names=expected_columns)
            total_inserted += len(batch)
            
            # Log progress for large datasets
            if start_idx % (batch_size * 10) == 0:  # Every 10 batches
                progress = (start_idx + len(batch)) / len(bulk_data) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_inserted:,} records)")
        
        logger.info(f"Successfully streamed {total_inserted:,} records into {table_name}")
        return total_inserted

    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get expected columns for normalized tables with time-based partitioning"""
        column_mapping = {
            'blocks': [
                'slot', 'proposer_index', 'parent_root', 'state_root', 'signature', 
                'version', 'timestamp_utc', 'randao_reveal', 'graffiti',
                'eth1_deposit_root', 'eth1_deposit_count', 'eth1_block_hash'
            ],
            'sync_aggregates': [
                'slot', 'sync_committee_bits', 'sync_committee_signature', 'timestamp_utc',
                'participating_validators'
            ],
            'execution_payloads': [
                'slot', 'parent_hash', 'fee_recipient', 'state_root', 'receipts_root',
                'logs_bloom', 'prev_randao', 'block_number', 'gas_limit', 'gas_used',
                'timestamp', 'base_fee_per_gas', 'block_hash', 'blob_gas_used', 
                'excess_blob_gas', 'extra_data'
            ],
            'transactions': [
                'slot', 'block_number', 'block_hash', 'transaction_index', 'transaction_hash', 
                'fee_recipient', 'gas_limit', 'gas_used', 'base_fee_per_gas', 'timestamp', 'timestamp_utc'
            ],
            'withdrawals': [
                'slot', 'block_number', 'block_hash', 'withdrawal_index', 'validator_index', 
                'address', 'amount', 'timestamp', 'timestamp_utc'
            ],
            'attestations': [
                'slot', 'attestation_index', 'aggregation_bits', 'signature', 'attestation_slot',
                'committee_index', 'beacon_block_root', 'source_epoch', 'source_root', 
                'target_epoch', 'target_root', 'timestamp_utc'
            ],
            'deposits': [
                'slot', 'deposit_index', 'proof', 'pubkey', 'withdrawal_credentials', 
                'amount', 'signature', 'timestamp_utc'
            ],
            'voluntary_exits': [
                'slot', 'exit_index', 'signature', 'epoch', 'validator_index', 'timestamp_utc'
            ],
            'proposer_slashings': [
                'slot', 'slashing_index', 'header_1_slot', 'header_1_proposer_index',
                'header_1_parent_root', 'header_1_state_root', 'header_1_body_root', 'header_1_signature',
                'header_2_slot', 'header_2_proposer_index', 'header_2_parent_root', 'header_2_state_root',
                'header_2_body_root', 'header_2_signature', 'timestamp_utc'
            ],
            'attester_slashings': [
                'slot', 'slashing_index', 'att_1_slot', 'att_1_committee_index', 'att_1_beacon_block_root',
                'att_1_source_epoch', 'att_1_target_epoch', 'att_1_signature', 'att_2_slot', 
                'att_2_committee_index', 'att_2_beacon_block_root', 'att_2_source_epoch', 
                'att_2_target_epoch', 'att_2_signature', 'timestamp_utc'
            ],
            'bls_changes': [
                'slot', 'change_index', 'signature', 'validator_index', 'from_bls_pubkey', 
                'to_execution_address', 'timestamp_utc'
            ],
            'blob_commitments': [
                'slot', 'commitment_index', 'commitment', 'timestamp_utc'
            ],
            'execution_requests': [
                'slot', 'request_type', 'request_index', 'pubkey', 'withdrawal_credentials', 
                'amount', 'signature', 'deposit_request_index', 'source_address', 'validator_pubkey',
                'source_pubkey', 'target_pubkey', 'timestamp_utc'
            ],
            'era_files_processed': [
                'era_number', 'network', 'file_hash', 'status', 'error_message', 
                'records_inserted', 'processed_at'
            ]
        }
        return column_mapping.get(table_name, [])

    def is_era_processed(self, network: str, era_number: int, file_hash: str) -> bool:
        """Check if era file has been processed successfully"""
        if era_number is None:
            era_number = 0
            
        try:
            result = self.client.query(
                "SELECT status FROM era_files_processed WHERE network = %s AND era_number = %s AND file_hash = %s ORDER BY processed_at DESC LIMIT 1",
                [network, int(era_number), file_hash]
            )
            
            if result.result_rows:
                return result.result_rows[0][0] == 'success'
            return False
        except Exception as e:
            logger.error(f"Error checking era processed status: {e}")
            return False

    def mark_era_processing(self, network: str, era_number: int, file_hash: str):
        """Mark era as being processed"""
        if era_number is None:
            era_number = 0
            
        try:
            self.client.insert(
                'era_files_processed',
                [[int(era_number), network, file_hash, 'processing', '', 0]],
                column_names=['era_number', 'network', 'file_hash', 'status', 'error_message', 'records_inserted']
            )
        except Exception as e:
            logger.error(f"Error marking era as processing: {e}")

    def mark_era_success(self, network: str, era_number: int, file_hash: str, records_count: int):
        """Mark era as successfully processed"""
        if era_number is None:
            era_number = 0
            
        try:
            self.client.insert(
                'era_files_processed',
                [[int(era_number), network, file_hash, 'success', '', records_count]],
                column_names=['era_number', 'network', 'file_hash', 'status', 'error_message', 'records_inserted']
            )
        except Exception as e:
            logger.error(f"Error marking era as success: {e}")

    def mark_era_failed(self, network: str, era_number: int, file_hash: str, error_message: str):
        """Mark era as failed"""
        if era_number is None:
            era_number = 0
            
        try:
            # Truncate error message
            error_msg = error_message[:500] if error_message else ''
            
            self.client.insert(
                'era_files_processed',
                [[int(era_number), network, file_hash, 'failed', error_msg, 0]],
                column_names=['era_number', 'network', 'file_hash', 'status', 'error_message', 'records_inserted']
            )
        except Exception as e:
            logger.error(f"Error marking era as failed: {e}")

    def get_processed_eras(self, network: str, start_era: int = None, end_era: int = None) -> List[int]:
        """Get list of successfully processed era numbers"""
        try:
            query = "SELECT era_number FROM era_files_processed WHERE network = %s AND status = 'success'"
            params = [network]
            
            if start_era is not None:
                query += " AND era_number >= %s"
                params.append(start_era)
                
            if end_era is not None:
                query += " AND era_number <= %s"
                params.append(end_era)
                
            query += " ORDER BY era_number"
            
            result = self.client.query(query, params)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            logger.error(f"Error getting processed eras: {e}")
            return []

    def get_failed_eras(self, network: str) -> List[Dict]:
        """Get list of failed era processing attempts"""
        try:
            result = self.client.query(
                "SELECT era_number, file_hash, processed_at, error_message FROM era_files_processed WHERE network = %s AND status = 'failed' ORDER BY processed_at DESC",
                [network]
            )
            
            failed_eras = []
            for row in result.result_rows:
                failed_eras.append({
                    'era_number': row[0],
                    'file_hash': row[1],
                    'processed_at': row[2],
                    'error_message': row[3]
                })
            return failed_eras
        except Exception as e:
            logger.error(f"Error getting failed eras: {e}")
            return []

    @staticmethod
    def calculate_file_hash(filepath: str) -> str:
        """Calculate hash of era file for tracking"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()