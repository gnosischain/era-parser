import os
import logging
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import clickhouse_connect

from .migrations import MigrationManager

logger = logging.getLogger(__name__)

class ClickHouseService:
    """Optimized service for beacon chain data with migration support and time-based partitioning"""

    # Single global batch size for all operations
    GLOBAL_BATCH_SIZE = 100000

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
        self._ensure_schema()

    def _connect(self):
        """Connect to ClickHouse with optimized settings for ClickHouse Cloud"""
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                secure=self.secure,
                verify=False,
                # OPTIMIZATION: Settings optimized for ClickHouse Cloud and large bulk operations
                settings={
                    'max_insert_block_size': 100000,  # Reduced from 1M for stability
                    'insert_quorum': 0,
                    'insert_quorum_timeout': 0,
                    'async_insert': 0,  # Disable async insert for more predictable behavior
                    'max_execution_time': 300,  # 5 minutes
                    'send_timeout': 300,  # 5 minutes  
                    'receive_timeout': 300,  # 5 minutes
                    'tcp_keep_alive_timeout': 60,
                    'connect_timeout': 60,
                    'max_memory_usage': 10000000000,  # 10GB
                },
                # Connection pool settings for stability
                connect_timeout=60,
                send_receive_timeout=300,  # 5 minutes for large operations
                compress=True,  # Enable compression for better network efficiency
            )
            client.command("SELECT 1")
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port} with cloud-optimized settings")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _ensure_schema(self):
        """Ensure database schema is up to date using migrations"""
        try:
            # Initialize migration manager
            migration_manager = MigrationManager(self.client, self.database)
            
            # Run all pending migrations
            success = migration_manager.run_migrations()
            
            if success:
                # Get migration status for logging
                status = migration_manager.get_migration_status()
                logger.info(f"Schema migrations completed: {status['applied_count']} applied, {status['pending_count']} pending")
            else:
                logger.warning("Some migrations failed, but continuing with existing schema")
                
        except Exception as e:
            logger.warning(f"Migration system failed, no fallback for ClickHouseService: {e}")
            # Note: No fallback here since era_state_manager.py handles its own fallback

    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status"""
        try:
            migration_manager = MigrationManager(self.client, self.database)
            return migration_manager.get_migration_status()
        except Exception as e:
            logger.error(f"Failed to get migration status: {e}")
            return {
                'applied_count': 0,
                'available_count': 0,
                'pending_count': 0,
                'last_applied': None,
                'pending_versions': [],
                'error': str(e)
            }

    def run_migrations(self, target_version: Optional[str] = None) -> bool:
        """Manually run migrations to target version"""
        try:
            migration_manager = MigrationManager(self.client, self.database)
            return migration_manager.run_migrations(target_version)
        except Exception as e:
            logger.error(f"Failed to run migrations: {e}")
            return False

    def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str) -> int:
        """Optimized bulk loading with single global batch size"""
        try:
            if df.empty:
                logger.warning(f"No data in DataFrame for {table_name}")
                return 0

            # Get expected columns for this table
            expected_columns = self._get_table_columns(table_name)
            
            # Convert to list of lists format that ClickHouse client expects
            bulk_data = self._prepare_bulk_data(df, expected_columns)
            
            if not bulk_data:
                logger.warning(f"No valid data prepared for {table_name}")
                return 0

            total_records = len(bulk_data)
            
            # SIMPLIFIED: Use global batch size for ALL tables - no special cases!
            if total_records > self.GLOBAL_BATCH_SIZE:
                # Use streaming for large datasets
                return self._streaming_bulk_insert(bulk_data, table_name, expected_columns)
            else:
                # Direct insert for small datasets with retry
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        self.client.insert(table_name, bulk_data, column_names=expected_columns)
                        logger.info(f"Bulk inserted {total_records} records into {table_name}")
                        return total_records
                    except Exception as e:
                        logger.warning(f"Bulk insert attempt {attempt + 1}/{max_retries} failed for {table_name}: {e}")
                        
                        if attempt < max_retries - 1:
                            import time
                            time.sleep(2 ** attempt)  # Exponential backoff
                            
                            # Try to reconnect
                            try:
                                self.client.command("SELECT 1")
                            except:
                                logger.info("Reconnecting to ClickHouse...")
                                self.client = self._connect()
                        else:
                            # If all retries fail, fall back to streaming
                            logger.warning(f"All bulk insert attempts failed, falling back to streaming for {table_name}")
                            return self._streaming_bulk_insert(bulk_data, table_name, expected_columns)
            
        except Exception as e:
            logger.error(f"Failed to load DataFrame into {table_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    def _prepare_bulk_data(self, df: pd.DataFrame, expected_columns: List[str]) -> List[List]:
        """Efficiently prepare data for bulk insert with single timestamp handling"""
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
        
        # Only timestamp_utc is a datetime column
        datetime_columns = {'timestamp_utc'}
        
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
            if value in ['1970-01-01T00:00:00+00:00', '1970-01-01T00:00:00Z', '1970-01-01T00:00:00', '0']:
                return datetime(1970, 1, 1)
            
            try:
                # First try to parse as Unix timestamp string
                try:
                    timestamp = int(value)
                    if timestamp > 0 and timestamp < 4294944000:  # Valid Unix timestamp range for ClickHouse
                        return datetime.fromtimestamp(timestamp)
                    elif timestamp == 0:
                        return datetime(1970, 1, 1)
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
                    
                    return datetime.fromisoformat(dt_str)
                else:
                    # Try to parse as float timestamp string
                    try:
                        timestamp = float(value)
                        if timestamp > 0 and timestamp < 4294944000:  # Valid Unix timestamp range
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
                if value > 0 and value < 4294944000:  # Valid Unix timestamp range
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
        """SIMPLIFIED: Handle large datasets with single global batch size"""
        total_inserted = 0
        
        logger.info(f"Streaming insert {len(bulk_data)} records into {table_name} with batch size {self.GLOBAL_BATCH_SIZE}")
        
        for start_idx in range(0, len(bulk_data), self.GLOBAL_BATCH_SIZE):
            batch = bulk_data[start_idx:start_idx + self.GLOBAL_BATCH_SIZE]
            
            # Retry logic for cloud reliability
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Insert with timeout handling
                    self.client.insert(table_name, batch, column_names=expected_columns)
                    total_inserted += len(batch)
                    break  # Success, exit retry loop
                    
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1}/{max_retries} failed for {table_name} batch {start_idx//self.GLOBAL_BATCH_SIZE + 1}: {e}")
                    
                    if attempt < max_retries - 1:
                        # Wait before retry and try to reconnect
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                        
                        try:
                            # Test connection and reconnect if needed
                            self.client.command("SELECT 1")
                        except:
                            logger.info("Reconnecting to ClickHouse...")
                            self.client = self._connect()
                    else:
                        # Final attempt failed
                        logger.error(f"All {max_retries} attempts failed for {table_name} batch {start_idx//self.GLOBAL_BATCH_SIZE + 1}")
                        raise
            
            # Progress logging for large datasets
            if len(bulk_data) > 50000 and (start_idx // self.GLOBAL_BATCH_SIZE) % 10 == 0:  # Every 10 batches for large datasets
                progress = (start_idx + len(batch)) / len(bulk_data) * 100
                logger.info(f"Progress: {progress:.1f}% ({total_inserted:,} records)")
        
        logger.info(f"Successfully streamed {total_inserted:,} records into {table_name}")
        return total_inserted

    def _get_table_columns(self, table_name: str) -> List[str]:
        """Get expected columns for normalized tables with SINGLE timestamp"""
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
                'timestamp_utc', 'base_fee_per_gas', 'block_hash', 'blob_gas_used', 
                'excess_blob_gas', 'extra_data'
            ],
            'transactions': [
                'slot', 'block_number', 'block_hash', 'transaction_index', 'transaction_hash', 
                'fee_recipient', 'gas_limit', 'gas_used', 'base_fee_per_gas', 'timestamp_utc'
            ],
            'withdrawals': [
                'slot', 'block_number', 'block_hash', 'withdrawal_index', 'validator_index', 
                'address', 'amount', 'timestamp_utc'
            ],
            'attestations': [
                'slot', 'attestation_index', 'aggregation_bits', 'signature', 'attestation_slot',
                'committee_index', 'beacon_block_root', 'source_epoch', 'source_root', 
                'target_epoch', 'target_root', 'timestamp_utc'
            ],
            'deposits': [
                'slot', 'deposit_index', 'pubkey', 'withdrawal_credentials', 
                'amount', 'signature', 'proof', 'timestamp_utc'
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
                'slot', 'slashing_index', 
                'att_1_slot', 'att_1_committee_index', 'att_1_beacon_block_root',
                'att_1_source_epoch', 'att_1_source_root', 'att_1_target_epoch', 'att_1_target_root',
                'att_1_signature', 'att_1_attesting_indices', 'att_1_validator_count',
                'att_2_slot', 'att_2_committee_index', 'att_2_beacon_block_root', 
                'att_2_source_epoch', 'att_2_source_root', 'att_2_target_epoch', 'att_2_target_root',
                'att_2_signature', 'att_2_attesting_indices', 'att_2_validator_count',
                'timestamp_utc', 'total_slashed_validators'
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
            'era_processing_state': [
                'era_filename', 'network', 'era_number', 'dataset', 'status', 
                'worker_id', 'attempt_count', 'file_hash', 'error_message', 
                'rows_inserted', 'processing_duration_ms'
            ]
        }
        return column_mapping.get(table_name, [])

    def get_processed_eras(self, network: str, start_era: int = None, end_era: int = None) -> List[int]:
        """Get list of successfully processed era numbers"""
        try:
            query = f"""
            SELECT era_number 
            FROM {self.database}.era_processing_progress 
            WHERE network = %s 
              AND completed_datasets = total_datasets
              AND completed_datasets > 0
            """
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
            # just return empty list if table doesn't exist yet
            logger.debug(f"Could not get processed eras (tables probably don't exist yet): {e}")
            return []

    def get_failed_eras(self, network: str) -> List[Dict]:
        """Get list of failed era processing attempts"""
        try:
            result = self.client.query(f"""
                SELECT era_number, era_filename, created_at, error_message, dataset
                FROM {self.database}.era_processing_state 
                WHERE network = %s AND status = 'failed' 
                ORDER BY created_at DESC
            """, [network])
            
            failed_eras = []
            for row in result.result_rows:
                failed_eras.append({
                    'era_number': row[0],
                    'era_filename': row[1],
                    'failed_at': row[2],
                    'error_message': row[3],
                    'dataset': row[4]
                })
            return failed_eras
        except Exception as e:
            # just return empty list if table doesn't exist yet
            logger.debug(f"Could not get failed eras (tables probably don't exist yet): {e}")
            return []

    @staticmethod
    def calculate_file_hash(filepath: str) -> str:
        """Calculate hash of era file for tracking"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()