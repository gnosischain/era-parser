"""Era data cleanup utilities"""

import logging
from typing import List, Set
from .era_slot_calculator import EraSlotCalculator

logger = logging.getLogger(__name__)

class EraDataCleaner:
    """Clean data for specific slot ranges"""
    
    # All beacon chain tables that store slot-based data
    BEACON_CHAIN_TABLES = [
        'blocks', 'sync_aggregates', 'execution_payloads', 'transactions',
        'withdrawals', 'attestations', 'deposits', 'voluntary_exits',
        'proposer_slashings', 'attester_slashings', 'bls_changes',
        'blob_commitments', 'execution_requests'
    ]
    
    def __init__(self, clickhouse_client, database: str):
        """
        Initialize data cleaner
        
        Args:
            clickhouse_client: ClickHouse client instance
            database: Database name
        """
        self.client = clickhouse_client
        self.database = database
    
    def clean_era_completely(self, network: str, era_number: int) -> None:
        """
        Remove ALL data for an era
        
        Args:
            network: Network name
            era_number: Era number to clean
        """
        slot_start, slot_end = EraSlotCalculator.get_era_slot_range(network, era_number)
        
        logger.info(f"Cleaning era {era_number} data (slots {slot_start}-{slot_end})")
        
        # Delete from all beacon chain tables
        for table in self.BEACON_CHAIN_TABLES:
            try:
                self.client.command(f"""
                    DELETE FROM {self.database}.{table} 
                    WHERE slot >= {slot_start} AND slot <= {slot_end}
                """)
                logger.debug(f"Cleaned {table} for era {era_number}")
            except Exception as e:
                logger.warning(f"Failed to clean {table} for era {era_number}: {e}")
        
        # Remove completion record
        try:
            self.client.command(f"""
                DELETE FROM {self.database}.era_completion 
                WHERE network = '{network}' AND era_number = {era_number}
            """)
            logger.debug(f"Removed completion record for era {era_number}")
        except Exception as e:
            logger.warning(f"Failed to remove completion record for era {era_number}: {e}")
    
    def clean_failed_eras(self, network: str) -> List[int]:
        """
        Clean all failed eras and return list
        
        Args:
            network: Network name
            
        Returns:
            List of era numbers that were cleaned
        """
        try:
            # Get failed eras
            result = self.client.query(f"""
                SELECT era_number FROM {self.database}.era_status 
                WHERE network = '{network}' AND status = 'failed'
                ORDER BY era_number
            """)
            
            failed_eras = [row[0] for row in result.result_rows]
            
            logger.info(f"Found {len(failed_eras)} failed eras to clean: {failed_eras}")
            
            # Clean each failed era
            for era_number in failed_eras:
                self.clean_era_completely(network, era_number)
            
            return failed_eras
            
        except Exception as e:
            logger.error(f"Failed to clean failed eras: {e}")
            return []
    
    def clean_slot_range(self, slot_start: int, slot_end: int) -> None:
        """
        Clean data for specific slot range
        
        Args:
            slot_start: Start slot (inclusive)
            slot_end: End slot (inclusive)
        """
        logger.info(f"Cleaning slot range {slot_start}-{slot_end}")
        
        for table in self.BEACON_CHAIN_TABLES:
            try:
                self.client.command(f"""
                    DELETE FROM {self.database}.{table} 
                    WHERE slot >= {slot_start} AND slot <= {slot_end}
                """)
                logger.debug(f"Cleaned {table} for slots {slot_start}-{slot_end}")
            except Exception as e:
                logger.warning(f"Failed to clean {table} for slots {slot_start}-{slot_end}: {e}")
    
    def optimize_tables_for_deduplication(self) -> None:
        """Run OPTIMIZE FINAL on all tables to deduplicate"""
        logger.info("Optimizing all tables for deduplication")
        
        for table in self.BEACON_CHAIN_TABLES:
            try:
                logger.info(f"Optimizing {table}...")
                self.client.command(f"OPTIMIZE TABLE {self.database}.{table} FINAL")
                logger.info(f"Optimized {table}")
            except Exception as e:
                logger.warning(f"Failed to optimize {table}: {e}")
        
        # Also optimize era completion table
        try:
            self.client.command(f"OPTIMIZE TABLE {self.database}.era_completion FINAL")
            logger.info("Optimized era_completion table")
        except Exception as e:
            logger.warning(f"Failed to optimize era_completion: {e}")
    
    def era_has_partial_data(self, network: str, era_number: int) -> bool:
        """
        Check if era has partial data in any table
        
        Args:
            network: Network name
            era_number: Era number
            
        Returns:
            True if era has partial data
        """
        slot_start, slot_end = EraSlotCalculator.get_era_slot_range(network, era_number)
        
        for table in self.BEACON_CHAIN_TABLES:
            try:
                result = self.client.query(f"""
                    SELECT count(*) FROM {self.database}.{table} 
                    WHERE slot >= {slot_start} AND slot <= {slot_end}
                """)
                
                count = result.result_rows[0][0] if result.result_rows else 0
                if count > 0:
                    return True
                    
            except Exception as e:
                logger.warning(f"Failed to check {table} for era {era_number}: {e}")
        
        return False
    
    def get_completed_eras(self, network: str, start_era: int = None, end_era: int = None) -> Set[int]:
        """
        Get truly completed eras from era_completion table
        
        Args:
            network: Network name
            start_era: Start era (optional)
            end_era: End era (optional)
            
        Returns:
            Set of completed era numbers
        """
        try:
            query = f"""
                SELECT era_number FROM {self.database}.era_status 
                WHERE network = '{network}' AND status = 'completed'
            """
            
            if start_era is not None:
                query += f" AND era_number >= {start_era}"
            if end_era is not None:
                query += f" AND era_number <= {end_era}"
                
            query += " ORDER BY era_number"
            
            result = self.client.query(query)
            return {row[0] for row in result.result_rows}
            
        except Exception as e:
            logger.warning(f"Failed to get completed eras: {e}")
            return set()