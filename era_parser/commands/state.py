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
        if not self.validate_required_args(args, 1, "era-parser --era-status [network]"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0] if args[0] != 'all' else None
        
        try:
            state_manager = EraStateManager()
            summary = state_manager.get_processing_summary(network)
            
            print(f"📊 Era Processing Status" + (f" ({network})" if network else " (All Networks)"))
            print("="*60)
            
            # Era-level summary
            if summary['era_summary']:
                print("\n📁 Era Summary:")
                for net, stats in summary['era_summary'].items():
                    print(f"  {net}:")
                    print(f"    Total Eras: {stats['total_eras']}")
                    print(f"    Fully Completed: {stats['fully_completed_eras']}")
                    print(f"    Processing: {stats['processing_eras']}")
                    print(f"    Failed: {stats['fully_failed_eras']}")
                    print(f"    Total Rows: {stats['total_rows']:,}")
                    
                    if stats['total_eras'] > 0:
                        completion_pct = (stats['fully_completed_eras'] / stats['total_eras']) * 100
                        print(f"    Completion: {completion_pct:.1f}%")
            
            # Dataset-level summary
            if summary['dataset_summary']:
                print("\n📊 Dataset Summary:")
                for net, datasets in summary['dataset_summary'].items():
                    print(f"  {net}:")
                    for dataset, stats in datasets.items():
                        print(f"    {dataset}:")
                        print(f"      Completed Eras: {stats['completed_eras']}")
                        print(f"      Failed Eras: {stats['failed_eras']}")
                        print(f"      Total Rows: {stats['total_rows']:,}")
                        print(f"      Highest Era: {stats['highest_completed_era']}")
            
            if not summary['era_summary'] and not summary['dataset_summary']:
                print("No processing data found.")
                
        except Exception as e:
            self.handle_error(e, "getting era status")
    
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