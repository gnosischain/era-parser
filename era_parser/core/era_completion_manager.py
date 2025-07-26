"""Era completion state management"""

import logging
from typing import List, Optional
from datetime import datetime
from .era_slot_calculator import EraSlotCalculator

logger = logging.getLogger(__name__)

class EraCompletionManager:
    """Manage era completion state"""
    
    def __init__(self, clickhouse_client, database: str):
        """
        Initialize completion manager
        
        Args:
            clickhouse_client: ClickHouse client instance
            database: Database name
        """
        self.client = clickhouse_client
        self.database = database
    
    def record_era_start(self, network: str, era_number: int) -> None:
        """
        Record that era processing has started
        
        Args:
            network: Network name
            era_number: Era number
        """
        slot_start, slot_end = EraSlotCalculator.get_era_slot_range(network, era_number)
        
        try:
            self.client.insert(
                f'{self.database}.era_completion',
                [[
                    network, era_number, 'failed',  # Will be updated to completed on success
                    slot_start, slot_end, 0, [],
                    datetime.now(), datetime.now(),
                    'Processing started', 0
                ]],
                column_names=[
                    'network', 'era_number', 'status', 'slot_start', 'slot_end',
                    'total_records', 'datasets_processed', 'processing_started_at',
                    'completed_at', 'error_message', 'retry_count'
                ]
            )
            logger.debug(f"Recorded start for era {era_number}")
        except Exception as e:
            logger.warning(f"Failed to record era start: {e}")
    
    def record_era_completion(self, network: str, era_number: int, 
                            datasets_processed: List[str], total_records: int) -> None:
        """
        Record successful era completion
        
        Args:
            network: Network name
            era_number: Era number
            datasets_processed: List of dataset names that were processed
            total_records: Total number of records inserted
        """
        slot_start, slot_end = EraSlotCalculator.get_era_slot_range(network, era_number)
        
        try:
            # Get current retry count
            retry_count = self._get_retry_count(network, era_number)
            
            self.client.insert(
                f'{self.database}.era_completion',
                [[
                    network, era_number, 'completed',
                    slot_start, slot_end, total_records, datasets_processed,
                    datetime.now(), datetime.now(),
                    '', retry_count
                ]],
                column_names=[
                    'network', 'era_number', 'status', 'slot_start', 'slot_end',
                    'total_records', 'datasets_processed', 'processing_started_at',
                    'completed_at', 'error_message', 'retry_count'
                ]
            )
            logger.info(f"Recorded completion for era {era_number}: {total_records} records, {len(datasets_processed)} datasets")
        except Exception as e:
            logger.error(f"Failed to record era completion: {e}")
            raise
    
    def record_era_failure(self, network: str, era_number: int, error_message: str) -> None:
        """
        Record era processing failure
        
        Args:
            network: Network name
            era_number: Era number
            error_message: Error message
        """
        slot_start, slot_end = EraSlotCalculator.get_era_slot_range(network, era_number)
        
        try:
            # Get current retry count and increment
            retry_count = self._get_retry_count(network, era_number) + 1
            
            # Truncate error message to prevent issues
            safe_error = error_message[:500] if error_message else "Unknown error"
            
            self.client.insert(
                f'{self.database}.era_completion',
                [[
                    network, era_number, 'failed',
                    slot_start, slot_end, 0, [],
                    datetime.now(), datetime.now(),
                    safe_error, retry_count
                ]],
                column_names=[
                    'network', 'era_number', 'status', 'slot_start', 'slot_end',
                    'total_records', 'datasets_processed', 'processing_started_at',
                    'completed_at', 'error_message', 'retry_count'
                ]
            )
            logger.error(f"Recorded failure for era {era_number} (attempt {retry_count}): {safe_error}")
        except Exception as e:
            logger.error(f"Failed to record era failure: {e}")
    
    def _get_retry_count(self, network: str, era_number: int) -> int:
        """Get current retry count for era"""
        try:
            result = self.client.query(f"""
                SELECT max(retry_count) FROM {self.database}.era_completion 
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            
            if result.result_rows and result.result_rows[0][0] is not None:
                return result.result_rows[0][0]
            return 0
        except Exception:
            return 0
    
    def get_era_status(self, network: str, era_number: int) -> Optional[dict]:
        """
        Get current status of an era
        
        Args:
            network: Network name
            era_number: Era number
            
        Returns:
            Era status dict or None if not found
        """
        try:
            result = self.client.query(f"""
                SELECT status, total_records, dataset_count, completed_at, retry_count, error_message
                FROM {self.database}.era_status 
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            
            if result.result_rows:
                row = result.result_rows[0]
                return {
                    'status': row[0],
                    'total_records': row[1],
                    'dataset_count': row[2],
                    'completed_at': row[3],
                    'retry_count': row[4],
                    'error_message': row[5]
                }
            return None
        except Exception as e:
            logger.warning(f"Failed to get era status: {e}")
            return None
    
    def get_era_summary(self, network: str, start_era: int = None, end_era: int = None) -> dict:
        """
        Get summary of era processing status
        
        Args:
            network: Network name
            start_era: Start era (optional)
            end_era: End era (optional)
            
        Returns:
            Summary dictionary
        """
        try:
            query = f"""
                SELECT 
                    status,
                    count(*) as count,
                    sum(total_records) as total_records
                FROM {self.database}.era_status 
                WHERE network = '{network}'
            """
            
            if start_era is not None:
                query += f" AND era_number >= {start_era}"
            if end_era is not None:
                query += f" AND era_number <= {end_era}"
                
            query += " GROUP BY status"
            
            result = self.client.query(query)
            
            summary = {'completed': 0, 'failed': 0, 'total_records': 0}
            
            for row in result.result_rows:
                status, count, records = row
                summary[status] = count
                if status == 'completed':
                    summary['total_records'] = records or 0
            
            summary['total_eras'] = summary['completed'] + summary['failed']
            
            return summary
            
        except Exception as e:
            logger.warning(f"Failed to get era summary: {e}")
            return {'completed': 0, 'failed': 0, 'total_eras': 0, 'total_records': 0}