"""
Simplified ClickHouse exporter with unified state management
"""

import json
import logging
import time
from typing import List, Dict, Any

import pandas as pd
from .base import BaseExporter
from .clickhouse_service import ClickHouseService
from .era_state_manager import EraStateManager

logger = logging.getLogger(__name__)

class ClickHouseExporter(BaseExporter):
    """Simplified ClickHouse exporter with unified state management"""

    def __init__(self, era_info: Dict[str, Any], era_file_path: str = None):
        """Initialize ClickHouse exporter"""
        super().__init__(era_info)
        self.era_file_path = era_file_path
        self.service = ClickHouseService()
        self.state_manager = EraStateManager()
        self.network = era_info.get('network', 'mainnet')
        
        # Get era_number from era_info
        self.era_number = era_info.get('era_number', 0)
        
        print(f"🔧 ClickHouse Exporter initialized for era {self.era_number}, network {self.network}")

    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export blocks to ClickHouse"""
        self.load_data_to_table(blocks, "blocks")

    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to ClickHouse"""
        self.load_data_to_table(data, data_type)

    def load_all_data_types(self, all_data: Dict[str, List[Dict[str, Any]]]):
        """
        Load all data types atomically using unified state management
        """
        if not all_data:
            logger.warning("No data to load")
            return

        print(f"🔄 Processing era {self.era_number} atomically")
        
        try:
            # 1. Clean FIRST using unified state manager
            self.state_manager.clean_era_data_if_needed(self.era_number, self.network)
            
            # 2. Mark as processing using unified state manager
            self.state_manager.record_era_start(self.era_number, self.network)
            
            # 3. Process all datasets
            datasets_processed = []
            total_records = 0
            
            print(f"📊 Loading all data types to ClickHouse:")
            for dataset, data_list in all_data.items():
                if not data_list:
                    continue
                    
                print(f"   📥 Loading {len(data_list)} records into {dataset}")
                records_loaded = self.load_data_to_table(data_list, dataset)
                
                if records_loaded > 0:
                    datasets_processed.append(dataset)
                    total_records += records_loaded
                    print(f"   ✅ {dataset}: {records_loaded} records loaded")
            
            # 4. Mark as completed using unified state manager
            self.state_manager.record_era_completion(
                self.era_number, self.network, datasets_processed, total_records
            )
            
            print(f"✅ Era {self.era_number} completed: {total_records} records, {len(datasets_processed)} datasets")
            
        except Exception as e:
            # 5. Mark as failed using unified state manager
            print(f"❌ Era {self.era_number} failed: {e}")
            self.state_manager.record_era_failure(self.era_number, self.network, str(e))
            raise

    def load_data_to_table(self, data: List[Dict[str, Any]], table_name: str) -> int:
        """Load data to a specific table"""
        if not data:
            return 0

        try:
            df = pd.DataFrame(data)
            return self.service.load_dataframe_to_table(df, table_name)
            
        except Exception as e:
            logger.error(f"Failed to load data into {table_name}: {e}")
            raise

    def is_era_completed(self) -> bool:
        """Check if era is already completed using unified state manager"""
        completed_eras = self.state_manager.get_completed_eras(self.network, self.era_number, self.era_number)
        return self.era_number in completed_eras

    # Legacy compatibility methods (not used in new system)
    def is_era_processed(self, target_datasets: List[str] = None) -> bool:
        """Legacy compatibility"""
        return self.is_era_completed()
    
    def get_pending_datasets_for_era(self, target_datasets: List[str] = None) -> List[str]:
        """Legacy compatibility"""
        if self.is_era_completed():
            return []
        return target_datasets or self.state_manager.ALL_DATASETS