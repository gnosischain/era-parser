"""
Unified era state management - consolidates all state operations
"""

import os
import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
from dataclasses import dataclass
import clickhouse_connect

from .migrations import MigrationManager

logger = logging.getLogger(__name__)

@dataclass
class EraStatus:
    """Simple era completion status"""
    era_number: int
    network: str
    status: str  # 'completed' or 'failed'
    slot_start: int
    slot_end: int
    total_records: int
    datasets_processed: List[str]
    completed_at: Optional[datetime] = None
    error_message: str = ""
    retry_count: int = 0

class EraStateManager:
    """Unified era state management with data cleanup and completion tracking"""
    
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
        """Create era completion tables using migration system"""
        try:
            migration_manager = MigrationManager(self.client, self.database)
            success = migration_manager.run_migrations()
            
            if success:
                logger.info("Era completion tables ensured via migrations")
                return True
            else:
                logger.error("Migration system failed")
                return False
                
        except Exception as e:
            logger.error(f"Migration-based table creation failed: {e}")
            return False

    def get_era_slot_range(self, era_number: int, network: str) -> Tuple[int, int]:
        """Calculate slot range for an era"""
        from ..config import get_network_config
        
        config = get_network_config(network)
        slots_per_era = config['SLOTS_PER_HISTORICAL_ROOT']
        
        start_slot = era_number * slots_per_era
        end_slot = start_slot + slots_per_era - 1
        
        return start_slot, end_slot

    # ===== COMPLETION TRACKING METHODS (from EraCompletionManager) =====
    
    def record_era_start(self, era_number: int, network: str) -> None:
        """Record that era processing has started"""
        if not self.tables_available:
            return
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            
            self.client.insert(
                f'{self.database}.era_completion',
                [[network, era_number, 'processing', start_slot, end_slot, 0, [], 
                datetime.now(), datetime.now(), 'Processing...', 0]],
                column_names=['network', 'era_number', 'status', 'slot_start', 'slot_end',
                            'total_records', 'datasets_processed', 'processing_started_at',
                            'completed_at', 'error_message', 'retry_count']
            )
            
            print(f"📝 Era {era_number} marked as 'processing'")
            
        except Exception as e:
            logger.error(f"Error recording era start: {e}")

    def record_era_completion(self, era_number: int, network: str, 
                            datasets_processed: List[str], total_records: int) -> None:
        """Record successful era completion"""
        if not self.tables_available:
            return
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            
            self.client.insert(
                f'{self.database}.era_completion',
                [[network, era_number, 'completed', start_slot, end_slot, total_records, 
                datasets_processed, datetime.now(), datetime.now(), '', 0]],
                column_names=['network', 'era_number', 'status', 'slot_start', 'slot_end',
                            'total_records', 'datasets_processed', 'processing_started_at',
                            'completed_at', 'error_message', 'retry_count']
            )
            
            print(f"✅ Era {era_number} marked as 'completed' with {total_records} records")
            
        except Exception as e:
            logger.error(f"Error recording era completion: {e}")

    def record_era_failure(self, era_number: int, network: str, error_message: str) -> None:
        """Record era processing failure"""
        if not self.tables_available:
            return
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            retry_count = self.get_era_retry_count(era_number, network) + 1
            
            self.client.insert(
                f'{self.database}.era_completion',
                [[network, era_number, 'failed', start_slot, end_slot, 0, [], 
                datetime.now(), datetime.now(), error_message[:500], retry_count]],
                column_names=['network', 'era_number', 'status', 'slot_start', 'slot_end',
                            'total_records', 'datasets_processed', 'processing_started_at',
                            'completed_at', 'error_message', 'retry_count']
            )
            
            print(f"❌ Era {era_number} marked as 'failed' (attempt {retry_count}): {error_message}")
            
        except Exception as e:
            logger.error(f"Error recording era failure: {e}")

    def get_era_retry_count(self, era_number: int, network: str) -> int:
        """Get current retry count for an era"""
        if not self.tables_available:
            return 0
            
        try:
            result = self.client.query(f"""
                SELECT COALESCE(MAX(retry_count), 0)
                FROM {self.database}.era_completion
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            
            return result.result_rows[0][0] if result.result_rows else 0
            
        except Exception as e:
            logger.error(f"Error getting retry count: {e}")
            return 0

    # ===== DATA CLEANING METHODS (from EraDataCleaner) =====
    
    def clean_era_completely(self, network: str, era_number: int) -> None:
        """Remove ALL data for an era"""
        if not self.tables_available:
            logger.warning("Tables not available, skipping cleanup")
            return
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            
            print(f"🧹 Cleaning era {era_number} data (slots {start_slot}-{end_slot})")
            
            # Delete from all beacon chain tables
            tables_cleaned = 0
            for table in self.ALL_DATASETS:
                try:
                    count_result = self.client.query(f"""
                        SELECT count(*) 
                        FROM {self.database}.{table} 
                        WHERE slot >= {start_slot} AND slot <= {end_slot}
                    """)
                    
                    record_count = count_result.result_rows[0][0] if count_result.result_rows else 0
                    
                    if record_count > 0:
                        print(f"   🗑️  Cleaning {record_count} records from {table}")
                        self.client.command(f"""
                            DELETE FROM {self.database}.{table} 
                            WHERE slot >= {start_slot} AND slot <= {end_slot}
                        """)
                        tables_cleaned += 1
                        
                except Exception as e:
                    print(f"   ⚠️  Could not clean {table}: {e}")
                    continue
            
            # Remove completion records
            self.client.command(f"""
                DELETE FROM {self.database}.era_completion 
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            
            print(f"✅ Cleaned era {era_number} ({tables_cleaned} tables had data)")
            
        except Exception as e:
            print(f"❌ Error cleaning era {era_number}: {e}")
            raise

    def clean_failed_eras(self, network: str) -> List[int]:
        """Clean all failed eras and return list"""
        failed_eras = self.get_failed_eras(network)
        
        for era_number in failed_eras:
            try:
                self.clean_era_completely(network, era_number)
                logger.info(f"Cleaned failed era {era_number}")
            except Exception as e:
                logger.error(f"Failed to clean era {era_number}: {e}")
                continue
        
        return failed_eras

    def era_has_partial_data(self, era_number: int, network: str) -> bool:
        """Check if era has any data in beacon chain tables"""
        if not self.tables_available:
            return False
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            
            # Check main tables for data
            for table in ['blocks', 'transactions', 'attestations']:
                try:
                    result = self.client.query(f"""
                        SELECT count(*) 
                        FROM {self.database}.{table} 
                        WHERE slot >= {start_slot} AND slot <= {end_slot} 
                        LIMIT 1
                    """)
                    
                    if result.result_rows and result.result_rows[0][0] > 0:
                        return True
                        
                except Exception:
                    continue
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking partial data for era {era_number}: {e}")
            return False

    def clean_era_data_if_needed(self, era_number: int, network: str) -> None:
        """Clean era data only if needed"""
        if not self.should_clean_era(era_number, network):
            print(f"✅ Era {era_number} already clean, skipping cleanup")
            return
        
        self.clean_era_completely(network, era_number)

    def should_clean_era(self, era_number: int, network: str) -> bool:
        """Check if era needs cleaning"""
        if not self.tables_available:
            return False
            
        try:
            start_slot, end_slot = self.get_era_slot_range(era_number, network)
            
            # Check for data in main tables
            for table in ['blocks', 'attestations', 'sync_aggregates']:
                try:
                    count_result = self.client.query(f"""
                        SELECT count(*) 
                        FROM {self.database}.{table} 
                        WHERE slot >= {start_slot} AND slot <= {end_slot}
                        LIMIT 1
                    """)
                    
                    if count_result.result_rows and count_result.result_rows[0][0] > 0:
                        return True
                        
                except Exception:
                    continue
            
            # Check for completion records
            completion_result = self.client.query(f"""
                SELECT count(*) 
                FROM {self.database}.era_completion 
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            
            if completion_result.result_rows and completion_result.result_rows[0][0] > 0:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if era {era_number} needs cleaning: {e}")
            return True

    def optimize_tables(self) -> None:
        """Optimize all tables for deduplication"""
        if not self.tables_available:
            logger.warning("Tables not available, skipping optimization")
            return
            
        logger.info("Optimizing tables for deduplication...")
        
        for table in self.ALL_DATASETS:
            try:
                logger.info(f"Optimizing {table}...")
                self.client.command(f"OPTIMIZE TABLE {self.database}.{table} FINAL")
                logger.info(f"Optimized {table}")
            except Exception as e:
                logger.warning(f"Could not optimize {table}: {e}")
                continue
        
        logger.info("Table optimization completed")

    # ===== STATE QUERYING METHODS =====

    def get_completed_eras(self, network: str, start_era: int = None, end_era: int = None) -> Set[int]:
        """Get set of completed era numbers"""
        if not self.tables_available:
            return set()
            
        try:
            query = f"""
                SELECT era_number
                FROM {self.database}.era_status
                WHERE network = '{network}' AND status = 'completed'
            """
            
            if start_era is not None:
                query += f" AND era_number >= {start_era}"
            if end_era is not None:
                query += f" AND era_number <= {end_era}"
                
            query += " ORDER BY era_number"
            
            result = self.client.query(query)
            completed = {row[0] for row in result.result_rows}
            
            print(f"📊 Found {len(completed)} completed eras for {network}")
            return completed
            
        except Exception as e:
            logger.error(f"Error getting completed eras: {e}")
            return set()

    def get_failed_eras(self, network: str) -> List[int]:
        """Get list of failed era numbers"""
        if not self.tables_available:
            return []
            
        try:
            result = self.client.query(f"""
                SELECT era_number
                FROM {self.database}.era_status
                WHERE network = '{network}' AND status = 'failed'
                ORDER BY era_number
            """)
            
            return [row[0] for row in result.result_rows]
            
        except Exception as e:
            logger.error(f"Error getting failed eras: {e}")
            return []

    def get_era_status_summary(self, network: str) -> Dict[str, Any]:
        """Get era processing summary for a network"""
        if not self.tables_available:
            return {'completed': 0, 'failed': 0, 'total_records': 0}
            
        try:
            result = self.client.query(f"""
                SELECT 
                    status,
                    count(*) as count,
                    sum(total_records) as total_records
                FROM {self.database}.era_status
                WHERE network = '{network}'
                GROUP BY status
            """)
            
            summary = {'completed': 0, 'failed': 0, 'total_records': 0}
            
            for row in result.result_rows:
                status, count, total_records = row
                summary[status] = count
                if status == 'completed':
                    summary['total_records'] = total_records or 0
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting era status summary: {e}")
            return {'completed': 0, 'failed': 0, 'total_records': 0}

    # ===== ERA SELECTION LOGIC (from ResumeHandler) =====
    
    def determine_eras_to_process(self, network: str, available_eras: List[Tuple[int, str]], 
                                 force: bool = False) -> List[Tuple[int, str]]:
        """
        Determine which eras need processing - UNIFIED LOGIC
        
        Args:
            network: Network name
            available_eras: List of (era_number, url) tuples
            force: Whether to force reprocess everything
            
        Returns:
            List of (era_number, url) tuples to process
        """
        all_era_numbers = [era_num for era_num, _ in available_eras]
        
        if not all_era_numbers:
            return []

        start_era = min(all_era_numbers)
        end_era = max(all_era_numbers)
        
        if force:
            # Force mode: clean and reprocess everything
            logger.info(f"Force mode: cleaning and reprocessing all {len(available_eras)} eras")
            for era_num, _ in available_eras:
                if self.era_has_partial_data(era_num, network):
                    self.clean_era_completely(network, era_num)
            return available_eras
        
        # Normal mode: skip completed eras
        completed_eras = self.get_completed_eras(network, start_era, end_era)
        
        incomplete_eras = []
        for era_num, url in available_eras:
            if era_num not in completed_eras:
                incomplete_eras.append((era_num, url))
                # Clean any partial data
                if self.era_has_partial_data(era_num, network):
                    logger.info(f"Cleaning partial data for era {era_num}")
                    self.clean_era_completely(network, era_num)
        
        logger.info(f"Normal mode: {len(completed_eras)} completed, {len(incomplete_eras)} to process")
        
        return incomplete_eras

    # ===== UTILITY METHODS =====
    
    @staticmethod
    def calculate_file_hash(filepath: str) -> str:
        """Calculate hash of era file for tracking"""
        import hashlib
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def get_era_filename_from_path(self, era_file_path: str) -> str:
        """Extract era filename from full path"""
        import os
        return os.path.basename(era_file_path)