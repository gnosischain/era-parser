"""
Era processing state management for granular dataset tracking.
Complete version with robust error handling and table creation.
"""

import os
import logging
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import clickhouse_connect

logger = logging.getLogger(__name__)

@dataclass
class EraDatasetState:
    """Represents the state of a specific dataset within an era file."""
    era_filename: str
    network: str
    era_number: int
    dataset: str
    status: str = "pending"  # pending, processing, completed, failed
    worker_id: str = ""
    attempt_count: int = 0
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    rows_inserted: Optional[int] = None
    file_hash: str = ""
    error_message: Optional[str] = None
    processing_duration_ms: Optional[int] = None

class EraStateManager:
    """Manages era file processing state with granular dataset tracking."""
    
    # All possible datasets that can be extracted from era files
    ALL_DATASETS = [
        'blocks', 'sync_aggregates', 'execution_payloads', 'transactions', 
        'withdrawals', 'attestations', 'deposits', 'voluntary_exits',
        'proposer_slashings', 'attester_slashings', 'bls_changes', 
        'blob_commitments', 'execution_requests'
    ]
    
    def __init__(self):
        """Initialize era state manager from environment variables"""
        self.host = os.getenv('CLICKHOUSE_HOST')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '8443'))
        self.user = os.getenv('CLICKHOUSE_USER', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD')
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'beacon_chain')
        self.secure = os.getenv('CLICKHOUSE_SECURE', 'true').lower() == 'true'

        if not self.host or not self.password:
            raise ValueError("CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD must be set")

        self.client = self._connect()
        self.tables_available = self._ensure_tables()

    def _connect(self):
        """Connect to ClickHouse"""
        try:
            client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                secure=self.secure,
                verify=False
            )
            client.command("SELECT 1")
            logger.info(f"Connected to ClickHouse for era state management")
            return client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def _ensure_tables(self) -> bool:
        """Create era processing state table and views - with error handling"""
        
        try:
            # Main state table
            self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.era_processing_state (
                `era_filename` String,              
                `network` String,                   
                `era_number` UInt32,               
                `dataset` String,                   
                `status` String,                    
                `worker_id` String DEFAULT '',     
                `attempt_count` UInt8 DEFAULT 0,   
                `created_at` DateTime DEFAULT now(),
                `completed_at` Nullable(DateTime),
                `rows_inserted` Nullable(UInt64),  
                `file_hash` String DEFAULT '',     
                `error_message` Nullable(String),
                `processing_duration_ms` Nullable(UInt64),
                `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),
                
                INDEX idx_status (status) TYPE minmax GRANULARITY 4,
                INDEX idx_network_dataset (network, dataset) TYPE minmax GRANULARITY 4,
                INDEX idx_era_number (era_number) TYPE minmax GRANULARITY 4
            ) ENGINE = ReplacingMergeTree(insert_version)
            PARTITION BY (network, toYYYYMM(created_at))
            ORDER BY (era_filename, dataset)
            SETTINGS index_granularity = 8192
            """)
            logger.info("Created era_processing_state table")

            # Era-level progress view
            self.client.command(f"""
            CREATE VIEW IF NOT EXISTS {self.database}.era_processing_progress AS
            SELECT 
                network,
                era_filename,
                era_number,
                countIf(status = 'completed') as completed_datasets,
                countIf(status = 'processing') as processing_datasets,
                countIf(status = 'failed') as failed_datasets,
                countIf(status = 'pending') as pending_datasets,
                count(*) as total_datasets,
                sum(rows_inserted) as total_rows_inserted,
                maxIf(completed_at, status = 'completed') as last_completed_at
            FROM {self.database}.era_processing_state
            GROUP BY network, era_filename, era_number
            """)
            logger.info("Created era_processing_progress view")

            # Dataset-level progress view  
            self.client.command(f"""
            CREATE VIEW IF NOT EXISTS {self.database}.dataset_processing_progress AS
            SELECT
                network,
                dataset,
                countIf(status = 'completed') as completed_eras,
                countIf(status = 'processing') as processing_eras,
                countIf(status = 'failed') as failed_eras,
                countIf(status = 'pending') as pending_eras,
                count(*) as total_eras,
                sum(rows_inserted) as total_rows_inserted,
                maxIf(era_number, status = 'completed') as highest_completed_era
            FROM {self.database}.era_processing_state  
            GROUP BY network, dataset
            """)
            logger.info("Created dataset_processing_progress view")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating era state tables/views: {e}")
            print(f"⚠️  Warning: Could not create era state management tables: {e}")
            print(f"   Era state management will be disabled for this session")
            return False

    def check_tables_exist(self) -> bool:
        """Check if era state management tables exist"""
        if not self.tables_available:
            return False
            
        try:
            # Check if the main table exists
            result = self.client.query(f"""
            SELECT count(*) 
            FROM system.tables 
            WHERE database = '{self.database}' 
              AND name = 'era_processing_state'
            """)
            
            table_exists = result.result_rows[0][0] > 0
            
            if not table_exists:
                logger.warning("era_processing_state table does not exist")
                return False
            
            # Check if the views exist
            result = self.client.query(f"""
            SELECT count(*) 
            FROM system.tables 
            WHERE database = '{self.database}' 
              AND name IN ('era_processing_progress', 'dataset_processing_progress')
            """)
            
            views_exist = result.result_rows[0][0] == 2
            
            if not views_exist:
                logger.warning("era_processing_progress or dataset_processing_progress views do not exist")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Error checking if tables exist: {e}")
            return False

    @staticmethod
    def calculate_file_hash(filepath: str) -> str:
        """Calculate hash of era file for tracking"""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def get_era_filename_from_path(self, era_file_path: str) -> str:
        """Extract era filename from full path"""
        import os
        return os.path.basename(era_file_path)

    def get_era_number_from_filename(self, era_filename: str) -> int:
        """Extract era number from filename"""
        parts = era_filename.replace('.era', '').split('-')
        
        # Handle different filename formats:
        # Format 1: network-XXXXX.era (e.g., gnosis-00001.era)
        # Format 2: network-XXXXX-hash.era (e.g., gnosis-00001-fe3b60d1.era)
        
        if len(parts) == 2:
            # Format 1: network-XXXXX.era
            try:
                return int(parts[1])  # Take the era number part
            except (ValueError, TypeError):
                return 0
        elif len(parts) >= 3:
            # Format 2: network-XXXXX-hash.era
            try:
                return int(parts[1])  # Take the era number part (second element)
            except (ValueError, TypeError):
                return 0
        
        # Fallback: try to find any 5-digit number in the filename
        import re
        match = re.search(r'-(\d{5})-?', era_filename)
        if match:
            return int(match.group(1))
        
        return 0

    def get_network_from_filename(self, era_filename: str) -> str:
        """Extract network from filename"""
        filename_lower = era_filename.lower()
        if 'gnosis' in filename_lower:
            return 'gnosis'
        elif 'sepolia' in filename_lower:
            return 'sepolia'
        else:
            return 'mainnet'

    def is_era_fully_processed(self, era_filename: str, target_datasets: List[str] = None) -> bool:
        """
        Check if era file is fully processed for all target datasets.
        Returns False if tables don't exist.
        """
        if not self.check_tables_exist():
            return False
            
        if target_datasets is None:
            target_datasets = self.ALL_DATASETS

        try:
            datasets_str = "','".join(target_datasets)
            result = self.client.query(f"""
            SELECT 
                dataset,
                status
            FROM {self.database}.era_processing_state
            WHERE era_filename = '{era_filename}'
              AND dataset IN ('{datasets_str}')
            ORDER BY dataset DESC, created_at DESC
            """)
            
            # Get latest status for each dataset
            dataset_statuses = {}
            for row in result.result_rows:
                dataset = row[0]
                status = row[1]
                if dataset not in dataset_statuses:  # Take most recent
                    dataset_statuses[dataset] = status
            
            # Check if all target datasets are completed
            for dataset in target_datasets:
                if dataset not in dataset_statuses or dataset_statuses[dataset] != 'completed':
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking era processing status: {e}")
            return False

    def get_pending_datasets(self, era_filename: str, target_datasets: List[str] = None) -> List[str]:
        """
        Get list of datasets that need processing for this era.
        Returns all datasets if tables don't exist.
        """
        if not self.check_tables_exist():
            return target_datasets or self.ALL_DATASETS
            
        if target_datasets is None:
            target_datasets = self.ALL_DATASETS

        try:
            datasets_str = "','".join(target_datasets)
            result = self.client.query(f"""
            SELECT 
                dataset,
                status
            FROM {self.database}.era_processing_state
            WHERE era_filename = '{era_filename}'
              AND dataset IN ('{datasets_str}')
            ORDER BY dataset DESC, created_at DESC
            """)
            
            # Get latest status for each dataset
            dataset_statuses = {}
            for row in result.result_rows:
                dataset = row[0]
                status = row[1]
                if dataset not in dataset_statuses:  # Take most recent
                    dataset_statuses[dataset] = status
            
            # Find datasets that need processing
            pending_datasets = []
            for dataset in target_datasets:
                if dataset not in dataset_statuses or dataset_statuses[dataset] != 'completed':
                    pending_datasets.append(dataset)
            
            return pending_datasets
            
        except Exception as e:
            logger.error(f"Error getting pending datasets: {e}")
            return target_datasets  # Return all if error

    def claim_dataset(self, era_filename: str, dataset: str, worker_id: str, 
                     file_hash: str = "") -> bool:
        """
        Atomically claim a dataset for processing.
        Returns False if tables don't exist.
        """
        if not self.check_tables_exist():
            return True  # Allow processing if no state management
            
        try:
            # Check current status
            result = self.client.query(f"""
            SELECT status, created_at
            FROM {self.database}.era_processing_state
            WHERE era_filename = '{era_filename}'
              AND dataset = '{dataset}'
            ORDER BY created_at DESC
            LIMIT 1
            """)
            
            if result.result_rows:
                status = result.result_rows[0][0]
                created_at = result.result_rows[0][1]
                
                # Don't claim if already completed
                if status == 'completed':
                    return False
                
                # Don't claim if recently processing (less than 30 minutes old)
                if status == 'processing' and created_at:
                    stale_threshold = datetime.now() - timedelta(minutes=30)
                    if created_at > stale_threshold:
                        return False
            
            # Extract era metadata
            network = self.get_network_from_filename(era_filename)
            era_number = self.get_era_number_from_filename(era_filename)
            
            # Claim the dataset
            self.client.insert(
                f'{self.database}.era_processing_state',
                [[era_filename, network, era_number, dataset, 'processing', worker_id, 0, file_hash, '', None]],
                column_names=['era_filename', 'network', 'era_number', 'dataset', 'status', 'worker_id', 'attempt_count', 'file_hash', 'error_message', 'processing_duration_ms']
            )
            
            logger.debug(f"Claimed dataset {dataset} for era {era_filename} (worker: {worker_id})")
            return True
            
        except Exception as e:
            logger.error(f"Error claiming dataset: {e}")
            return True  # Allow processing if claim fails

    def complete_dataset(self, era_filename: str, dataset: str, rows_inserted: int = 0, 
                        processing_duration_ms: int = None) -> None:
        """
        Mark a dataset as completed.
        Silently fails if tables don't exist.
        """
        if not self.check_tables_exist():
            return
            
        try:
            network = self.get_network_from_filename(era_filename)
            era_number = self.get_era_number_from_filename(era_filename)
            
            self.client.insert(
                f'{self.database}.era_processing_state',
                [[era_filename, network, era_number, dataset, 'completed', '', 0, '', '', rows_inserted, processing_duration_ms]],
                column_names=['era_filename', 'network', 'era_number', 'dataset', 'status', 'worker_id', 'attempt_count', 'file_hash', 'error_message', 'rows_inserted', 'processing_duration_ms']
            )
            
            logger.debug(f"Completed dataset {dataset} for era {era_filename} ({rows_inserted} rows)")
            
        except Exception as e:
            logger.error(f"Error completing dataset: {e}")

    def fail_dataset(self, era_filename: str, dataset: str, error_message: str) -> None:
        """
        Mark a dataset as failed.
        Silently fails if tables don't exist.
        """
        if not self.check_tables_exist():
            return
            
        try:
            network = self.get_network_from_filename(era_filename)
            era_number = self.get_era_number_from_filename(era_filename)
            
            # Get current attempt count
            result = self.client.query(f"""
            SELECT COALESCE(MAX(attempt_count), 0) + 1
            FROM {self.database}.era_processing_state
            WHERE era_filename = '{era_filename}'
              AND dataset = '{dataset}'
            """)
            next_attempt = result.result_rows[0][0] if result.result_rows else 1
            
            # Truncate and escape error message
            safe_error = error_message[:500] if error_message else ""
            safe_error = safe_error.replace("'", "''")
            
            self.client.insert(
                f'{self.database}.era_processing_state',
                [[era_filename, network, era_number, dataset, 'failed', '', next_attempt, '', safe_error, None, None]],
                column_names=['era_filename', 'network', 'era_number', 'dataset', 'status', 'worker_id', 'attempt_count', 'file_hash', 'error_message', 'rows_inserted', 'processing_duration_ms']
            )
            
            logger.error(f"Failed dataset {dataset} for era {era_filename} (attempt {next_attempt}): {error_message}")
            
        except Exception as e:
            logger.error(f"Error marking dataset as failed: {e}")

    def get_processing_summary(self, network: str = None) -> Dict[str, Any]:
        """
        Get processing summary across all eras and datasets.
        Returns empty summary if tables don't exist.
        """
        if not self.check_tables_exist():
            return {'era_summary': {}, 'dataset_summary': {}}
            
        try:
            network_filter = f"AND network = '{network}'" if network else ""
            
            # Era-level summary
            era_result = self.client.query(f"""
            SELECT 
                network,
                COUNT(DISTINCT era_filename) as total_eras,
                countIf(completed_datasets = total_datasets) as fully_completed_eras,
                countIf(processing_datasets > 0) as processing_eras,
                countIf(failed_datasets > 0 AND completed_datasets = 0) as fully_failed_eras,
                SUM(total_rows_inserted) as total_rows
            FROM {self.database}.era_processing_progress
            WHERE 1=1 {network_filter}
            GROUP BY network
            """)
            
            # Dataset-level summary
            dataset_result = self.client.query(f"""
            SELECT 
                network,
                dataset,
                completed_eras,
                failed_eras,
                total_rows_inserted,
                highest_completed_era
            FROM {self.database}.dataset_processing_progress
            WHERE 1=1 {network_filter}
            ORDER BY network, dataset
            """)
            
            summary = {
                'era_summary': {},
                'dataset_summary': {}
            }
            
            # Process era summary
            for row in era_result.result_rows:
                net = row[0]
                summary['era_summary'][net] = {
                    'total_eras': row[1],
                    'fully_completed_eras': row[2],
                    'processing_eras': row[3],
                    'fully_failed_eras': row[4],
                    'total_rows': row[5]
                }
            
            # Process dataset summary
            for row in dataset_result.result_rows:
                net = row[0]
                dataset = row[1]
                if net not in summary['dataset_summary']:
                    summary['dataset_summary'][net] = {}
                
                summary['dataset_summary'][net][dataset] = {
                    'completed_eras': row[2],
                    'failed_eras': row[3],
                    'total_rows': row[4],
                    'highest_completed_era': row[5]
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting processing summary: {e}")
            return {'era_summary': {}, 'dataset_summary': {}}

    def get_failed_datasets(self, network: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get list of failed datasets for retry.
        Returns empty list if tables don't exist.
        """
        if not self.check_tables_exist():
            return []
            
        try:
            network_filter = f"AND network = '{network}'" if network else ""
            
            result = self.client.query(f"""
            SELECT 
                era_filename,
                network,
                era_number,
                dataset,
                attempt_count,
                error_message,
                created_at
            FROM {self.database}.era_processing_state
            WHERE status = 'failed' {network_filter}
            ORDER BY created_at DESC
            LIMIT {limit}
            """)
            
            failed_datasets = []
            for row in result.result_rows:
                failed_datasets.append({
                    'era_filename': row[0],
                    'network': row[1],
                    'era_number': row[2],
                    'dataset': row[3],
                    'attempt_count': row[4],
                    'error_message': row[5],
                    'created_at': row[6]
                })
            
            return failed_datasets
            
        except Exception as e:
            logger.error(f"Error getting failed datasets: {e}")
            return []

    def cleanup_stale_processing(self, timeout_minutes: int = 30) -> int:
        """
        Reset stale processing entries back to pending.
        Returns 0 if tables don't exist.
        """
        if not self.check_tables_exist():
            return 0
            
        try:
            stale_threshold = datetime.now() - timedelta(minutes=timeout_minutes)
            
            # Find stale processing entries
            result = self.client.query(f"""
            SELECT era_filename, dataset, worker_id, created_at
            FROM {self.database}.era_processing_state
            WHERE status = 'processing'
              AND created_at < '{stale_threshold.strftime('%Y-%m-%d %H:%M:%S')}'
            """)
            
            reset_count = 0
            for row in result.result_rows:
                era_filename, dataset, worker_id, created_at = row
                
                # Check if there's a newer completed entry
                check_result = self.client.query(f"""
                SELECT COUNT(*)
                FROM {self.database}.era_processing_state
                WHERE era_filename = '{era_filename}'
                  AND dataset = '{dataset}'
                  AND status = 'completed'
                  AND created_at > '{created_at.strftime('%Y-%m-%d %H:%M:%S')}'
                """)
                
                if check_result.result_rows[0][0] > 0:
                    continue  # Already completed
                
                # Reset to pending
                network = self.get_network_from_filename(era_filename)
                era_number = self.get_era_number_from_filename(era_filename)
                
                self.client.insert(
                    f'{self.database}.era_processing_state',
                    [[era_filename, network, era_number, dataset, 'pending', '', 0, '', f'Reset from stale processing (worker: {worker_id})', None, None]],
                    column_names=['era_filename', 'network', 'era_number', 'dataset', 'status', 'worker_id', 'attempt_count', 'file_hash', 'error_message', 'rows_inserted', 'processing_duration_ms']
                )
                
                reset_count += 1
                logger.info(f"Reset stale processing: {era_filename}/{dataset} (worker: {worker_id})")
            
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stale processing entries")
            
            return reset_count
            
        except Exception as e:
            logger.error(f"Error cleaning up stale processing: {e}")
            return 0