import os
from typing import List

from .base import BaseCommand

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
    """Handler for era state management operations using unified state manager"""
    
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
        if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
            print("🔧 ClickHouse environment not detected, loading from .env file...")
            load_env_file()
            
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
        """Handle cleaning failed eras using unified state manager"""
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
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            failed = state_manager.get_failed_eras(network)
            
            print(f"❌ Failed Eras" + (f" ({network})" if network else " (All Networks)"))
            print("="*60)
            
            if not failed:
                print("No failed eras found.")
                return
            
            # Show up to limit
            for era_number in failed[:limit]:
                print(f"Era: {era_number}")
                print(f"  Network: {network}")
                print()
                
        except Exception as e:
            self.handle_error(e, "getting failed eras")
    
    def _handle_era_cleanup(self, args: List[str]) -> None:
        """Handle era cleanup operations using unified state manager"""
        if not self._ensure_environment_loaded():
            return
            
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            print("🔧 Optimizing tables for deduplication...")
            state_manager.optimize_tables()
            print("✅ Table optimization completed")
                
        except Exception as e:
            self.handle_error(e, "cleaning up")
    
    def _handle_era_check(self, args: List[str]) -> None:
        """Handle era status check for specific era using unified state manager"""
        if not self.validate_required_args(args, 2, "era-parser --era-check <network> <era_number>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        try:
            era_number = int(args[1])
        except ValueError:
            print("❌ Era number must be an integer")
            return
        
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            completed_eras = state_manager.get_completed_eras(network, era_number, era_number)
            failed_eras = state_manager.get_failed_eras(network)
            
            print(f"📋 Era Status: {network} era {era_number}")
            print("="*60)
            
            if era_number in completed_eras:
                print("Status: ✅ Completed")
            elif era_number in failed_eras:
                print("Status: ❌ Failed")
            else:
                print("Status: ⏸️  Not processed")
                
        except Exception as e:
            self.handle_error(e, "checking era status")