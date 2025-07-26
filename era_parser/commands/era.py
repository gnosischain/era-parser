"""Era management commands for recovery and validation"""

import os
from typing import List

from .base import BaseCommand
from ..core.resume_handler import ResumeHandler
from ..core.era_data_cleaner import EraDataCleaner
from ..core.era_completion_manager import EraCompletionManager

def load_env_file(env_file_path: str = '.env'):
    """Load environment variables from .env file"""
    if os.path.exists(env_file_path):
        print(f"📁 Loading environment from {env_file_path}")
        with open(env_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:
                        os.environ[key] = value
                        print(f"   ✅ Set {key}")
    else:
        print(f"   ℹ️  No .env file found at {env_file_path}")

class EraCommand(BaseCommand):
    """Handler for era management operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute era management command"""
        if not args:
            print("❌ Era command requires arguments")
            return
        
        command_type = args[0]
        
        if command_type == "--clean-failed":
            self._handle_clean_failed(args[1:])
        elif command_type == "--clean-era":
            self._handle_clean_era(args[1:])
        elif command_type == "--optimize-tables":
            self._handle_optimize_tables(args[1:])
        elif command_type == "--validate":
            self._handle_validate(args[1:])
        else:
            print(f"❌ Unknown era command: {command_type}")
    
    def _ensure_environment_loaded(self):
        """Ensure environment variables are loaded"""
        if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
            print("🔧 ClickHouse environment not detected, loading from .env file...")
            load_env_file()
            
            if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
                print("❌ ClickHouse environment variables not found!")
                print("💡 Make sure to set CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD in your .env file")
                return False
        return True
    
    def _handle_clean_failed(self, args: List[str]) -> None:
        """Handle cleaning failed eras"""
        if not self.validate_required_args(args, 1, "era-parser --clean-failed <network>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        
        try:
            from ..export.clickhouse_service import ClickHouseService
            ch_service = ClickHouseService()
            resume_handler = ResumeHandler(ch_service.client, ch_service.database)
            
            failed_eras = resume_handler.clean_failed_eras(network)
            
            if failed_eras:
                print(f"✅ Cleaned {len(failed_eras)} failed eras: {failed_eras}")
            else:
                print("No failed eras found to clean")
                
        except Exception as e:
            self.handle_error(e, "cleaning failed eras")
    
    def _handle_clean_era(self, args: List[str]) -> None:
        """Handle cleaning specific eras"""
        if not self.validate_required_args(args, 2, "era-parser --clean-era <network> <era_range>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        era_range = args[1]
        
        try:
            # Parse era range
            if '-' in era_range:
                start_era, end_era = map(int, era_range.split('-'))
                era_numbers = list(range(start_era, end_era + 1))
            else:
                era_numbers = [int(era_range)]
            
            from ..export.clickhouse_service import ClickHouseService
            ch_service = ClickHouseService()
            data_cleaner = EraDataCleaner(ch_service.client, ch_service.database)
            
            print(f"🗑️  Cleaning {len(era_numbers)} eras for {network}")
            
            for era_number in era_numbers:
                data_cleaner.clean_era_completely(network, era_number)
                print(f"   ✅ Cleaned era {era_number}")
            
            print(f"✅ Successfully cleaned {len(era_numbers)} eras")
            
        except Exception as e:
            self.handle_error(e, "cleaning eras")
    
    def _handle_optimize_tables(self, args: List[str]) -> None:
        """Handle table optimization"""
        if not self._ensure_environment_loaded():
            return
        
        try:
            from ..export.clickhouse_service import ClickHouseService
            ch_service = ClickHouseService()
            data_cleaner = EraDataCleaner(ch_service.client, ch_service.database)
            
            print("🔧 Optimizing all tables for deduplication...")
            data_cleaner.optimize_tables_for_deduplication()
            print("✅ Table optimization completed")
            
        except Exception as e:
            self.handle_error(e, "optimizing tables")
    
    def _handle_validate(self, args: List[str]) -> None:
        """Handle era validation"""
        if not self.validate_required_args(args, 2, "era-parser --validate <network> <era_range>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        era_range = args[1]
        
        try:
            # Parse era range
            if '-' in era_range:
                start_era, end_era = map(int, era_range.split('-'))
                era_numbers = list(range(start_era, end_era + 1))
            else:
                era_numbers = [int(era_range)]
            
            from ..export.clickhouse_service import ClickHouseService
            ch_service = ClickHouseService()
            completion_manager = EraCompletionManager(ch_service.client, ch_service.database)
            data_cleaner = EraDataCleaner(ch_service.client, ch_service.database)
            
            print(f"🔍 Validating {len(era_numbers)} eras for {network}")
            
            completed_count = 0
            failed_count = 0
            missing_count = 0
            
            for era_number in era_numbers:
                status = completion_manager.get_era_status(network, era_number)
                
                if status is None:
                    print(f"   ❓ Era {era_number}: No completion record")
                    missing_count += 1
                elif status['status'] == 'completed':
                    print(f"   ✅ Era {era_number}: Completed ({status['total_records']} records)")
                    completed_count += 1
                elif status['status'] == 'failed':
                    print(f"   ❌ Era {era_number}: Failed - {status['error_message'][:50]}...")
                    failed_count += 1
            
            print(f"\n📊 Validation Summary:")
            print(f"   Completed: {completed_count}")
            print(f"   Failed: {failed_count}")
            print(f"   Missing: {missing_count}")
            
        except Exception as e:
            self.handle_error(e, "validating eras")