"""Resume logic for era processing"""

import logging
from typing import List, Set, Tuple
from .era_data_cleaner import EraDataCleaner
from .era_completion_manager import EraCompletionManager

logger = logging.getLogger(__name__)

class ResumeHandler:
    """Handle resume logic for era processing"""
    
    def __init__(self, clickhouse_client, database: str):
        """
        Initialize resume handler
        
        Args:
            clickhouse_client: ClickHouse client instance
            database: Database name
        """
        self.data_cleaner = EraDataCleaner(clickhouse_client, database)
        self.completion_manager = EraCompletionManager(clickhouse_client, database)
    
    def get_eras_to_process(self, network: str, available_eras: List[Tuple[int, str]], 
                           resume: bool = False, force: bool = False) -> List[Tuple[int, str]]:
        """
        Determine which eras need processing
        
        Args:
            network: Network name
            available_eras: List of (era_number, url) tuples
            resume: Whether to resume (skip completed eras)
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
            # Force mode: process everything, clean all first
            logger.info(f"Force mode: cleaning and reprocessing all {len(available_eras)} eras")
            for era_num, _ in available_eras:
                if self.data_cleaner.era_has_partial_data(network, era_num):
                    self.data_cleaner.clean_era_completely(network, era_num)
            return available_eras
        
        if not resume:
            # Normal mode: process everything
            logger.info(f"Normal mode: processing all {len(available_eras)} eras")
            return available_eras
        
        # Resume mode: skip completed eras, clean incomplete ones
        completed_eras = self.data_cleaner.get_completed_eras(network, start_era, end_era)
        
        incomplete_eras = []
        for era_num, url in available_eras:
            if era_num not in completed_eras:
                incomplete_eras.append((era_num, url))
                # Clean any partial data for incomplete eras
                if self.data_cleaner.era_has_partial_data(network, era_num):
                    logger.info(f"Cleaning partial data for era {era_num}")
                    self.data_cleaner.clean_era_completely(network, era_num)
        
        logger.info(f"Resume mode: {len(completed_eras)} completed, {len(incomplete_eras)} to process")
        
        return incomplete_eras
    
    def clean_failed_eras(self, network: str) -> List[int]:
        """
        Clean all failed eras
        
        Args:
            network: Network name
            
        Returns:
            List of cleaned era numbers
        """
        return self.data_cleaner.clean_failed_eras(network)
    
    def get_processing_summary(self, network: str, start_era: int = None, end_era: int = None) -> dict:
        """
        Get processing summary for eras
        
        Args:
            network: Network name
            start_era: Start era (optional)
            end_era: End era (optional)
            
        Returns:
            Summary dictionary
        """
        return self.completion_manager.get_era_summary(network, start_era, end_era)