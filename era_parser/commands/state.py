import os
from typing import List

from .base import BaseCommand
from ..export.era_state_manager import EraStateManager

def load_env_file(env_file_path: str = '.env'):
    """Load environment variables from .env file"""
    if os.path.exists(env_file_path):
        print(f"📁 Loading environment from {env_file_path}")
        with open(env_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Only set if not already in environment
                    if key not in os.environ:
                        os.environ[key] = value
                        print(f"   ✅ Set {key}")
    else:
        print(f"   ℹ️  No .env file found at {env_file_path}")

class StateCommand(BaseCommand):
    """Handler for era state management operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute state management command"""
        if not args:
            print("❌ State command requires arguments")
            return
        
        command_type = args[0]
        
        if command_type == "--era-status":
            self._handle_era_status(args[1:])
        elif command_type == "--era-failed":
            self._handle_era_failed(args[1:])
        elif command_type == "--era-cleanup":
            self._handle_era_cleanup(args[1:])
        elif command_type == "--era-check":
            self._handle_era_check(args[1:])
        elif command_type == "--clean-failed-eras":
            self._handle_clean_failed_eras(args[1:])
        else:
            print(f"❌ Unknown state command: {command_type}")
    
    def _ensure_environment_loaded(self):
        """Ensure environment variables are loaded before creating EraStateManager"""
        # Check if ClickHouse variables are already set
        if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
            print("🔧 ClickHouse environment not detected, loading from .env file...")
            load_env_file()
            
            # Check again after loading
            if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
                print("❌ ClickHouse environment variables not found!")
                print("💡 Make sure to set CLICKHOUSE_HOST and CLICKHOUSE_PASSWORD in your .env file")
                return False
        return True
    
    def _handle_era_status(self, args: List[str]) -> None:
        """Handle era status display"""
        if not self.validate_required_args(args, 1, "era-parser --era-status [network] [era_range]"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0] if args[0] != 'all' else None
        era_range = args[1] if len(args) > 1 else None
        
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            if era_range:
                # Show specific era range
                if '-' in era_range:
                    start_str, end_str = era_range.split('-', 1)
                    start_era, end_era = int(start_str), int(end_str)
                else:
                    start_era = end_era = int(era_range)
                
                completed = state_manager.get_completed_eras(network, start_era, end_era)
                failed = state_manager.get_failed_eras(network)
                failed_in_range = [era for era in failed if start_era <= era <= end_era]
                
                print(f"📊 Era Status for {network} ({start_era}-{end_era})")
                print("="*60)
                print(f"✅ Completed: {len(completed)} eras")
                print(f"❌ Failed: {len(failed_in_range)} eras")
                print(f"⏸️  Not processed: {(end_era - start_era + 1) - len(completed) - len(failed_in_range)} eras")
                
                if failed_in_range:
                    print(f"\nFailed eras: {failed_in_range}")
            else:
                # Show summary
                summary = state_manager.get_era_status_summary(network)
                
                print(f"📊 Era Processing Summary" + (f" ({network})" if network else " (All Networks)"))
                print("="*60)
                print(f"✅ Completed eras: {summary['completed']}")
                print(f"❌ Failed eras: {summary['failed']}")
                print(f"📊 Total records: {summary['total_records']:,}")
                    
        except Exception as e:
            self.handle_error(e, "getting era status")

    def _handle_clean_failed_eras(self, args: List[str]) -> None:
        """Handle cleaning failed eras"""
        if not self.validate_required_args(args, 1, "era-parser --clean-failed-eras <network>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            failed_eras = state_manager.clean_failed_eras(network)
            
            if failed_eras:
                print(f"🧹 Cleaned {len(failed_eras)} failed eras: {failed_eras}")
            else:
                print(f"✅ No failed eras found for {network}")
                
        except Exception as e:
            self.handle_error(e, "cleaning failed eras")
    
    def _handle_era_failed(self, args: List[str]) -> None:
        """Handle failed era datasets display"""
        if not self.validate_required_args(args, 1, "era-parser --era-failed [network] [limit]"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0] if args[0] != 'all' else None
        limit = int(args[1]) if len(args) > 1 else 20
        
        try:
            state_manager = EraStateManager()
            failed = state_manager.get_failed_datasets(network, limit)
            
            print(f"❌ Failed Datasets" + (f" ({network})" if network else " (All Networks)"))
            print("="*60)
            
            if not failed:
                print("No failed datasets found.")
                return
            
            for failure in failed:
                print(f"Era: {failure['era_filename']}")
                print(f"  Dataset: {failure['dataset']}")
                print(f"  Network: {failure['network']}")
                print(f"  Era Number: {failure['era_number']}")
                print(f"  Attempts: {failure['attempt_count']}")
                print(f"  Error: {failure['error_message'][:100]}...")
                print(f"  Failed At: {failure['created_at']}")
                print()
                
        except Exception as e:
            self.handle_error(e, "getting failed datasets")
    
    def _handle_era_cleanup(self, args: List[str]) -> None:
        """Handle era cleanup operations"""
        if not self._ensure_environment_loaded():
            return
            
        timeout = int(args[0]) if args else 30
        
        try:
            state_manager = EraStateManager()
            reset_count = state_manager.cleanup_stale_processing(timeout)
            
            if reset_count > 0:
                print(f"✅ Reset {reset_count} stale processing entries")
            else:
                print("No stale processing entries found")
                
        except Exception as e:
            self.handle_error(e, "cleaning up stale processing")
    
    def _handle_era_check(self, args: List[str]) -> None:
        """Handle era status check for specific file"""
        if not self.validate_required_args(args, 1, "era-parser --era-check <era_file>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        era_file = args[0]
        
        try:
            state_manager = EraStateManager()
            era_filename = state_manager.get_era_filename_from_path(era_file)
            
            # Check if fully processed
            is_complete = state_manager.is_era_fully_processed(era_filename)
            pending_datasets = state_manager.get_pending_datasets(era_filename)
            
            print(f"📋 Era Status: {era_filename}")
            print("="*60)
            print(f"Fully Processed: {'✅ Yes' if is_complete else '❌ No'}")
            
            if pending_datasets:
                print(f"Pending Datasets ({len(pending_datasets)}):")
                for dataset in pending_datasets:
                    print(f"  - {dataset}")
            else:
                print("All datasets completed ✅")
                
        except Exception as e:
            self.handle_error(e, "checking era status")