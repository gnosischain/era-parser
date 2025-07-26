"""Simple recovery commands"""

import os
from typing import List

from .base import BaseCommand

def load_env_file(env_file_path: str = '.env'):
    """Load environment variables from .env file"""
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if key not in os.environ:
                        os.environ[key] = value

class RecoveryCommand(BaseCommand):
    """Simple recovery operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute recovery command"""
        if not args:
            print("❌ Recovery command requires arguments")
            return
        
        command_type = args[0]
        
        if command_type == "--reset-failed":
            self._handle_reset_failed(args[1:])
        elif command_type == "--optimize":
            self._handle_optimize(args[1:])
        else:
            print(f"❌ Unknown recovery command: {command_type}")
    
    def _ensure_environment_loaded(self):
        """Ensure environment variables are loaded"""
        if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
            load_env_file()
            
            if not os.getenv('CLICKHOUSE_HOST') or not os.getenv('CLICKHOUSE_PASSWORD'):
                print("❌ ClickHouse environment variables not found!")
                return False
        return True
    
    def _handle_reset_failed(self, args: List[str]) -> None:
        """Reset failed processing states to allow retry"""
        if not self.validate_required_args(args, 1, "era-parser --reset-failed <network>"):
            return
        
        if not self._ensure_environment_loaded():
            return
        
        network = args[0]
        
        try:
            from ..export.era_state_manager import EraStateManager
            state_manager = EraStateManager()
            
            # Reset failed datasets back to pending
            reset_count = state_manager.cleanup_stale_processing(0)  # Reset all
            
            print(f"✅ Reset {reset_count} failed processing states for {network}")
            print("💡 You can now retry with --resume")
            
        except Exception as e:
            self.handle_error(e, "resetting failed states")
    
    def _handle_optimize(self, args: List[str]) -> None:
        """Optimize ClickHouse tables"""
        if not self._ensure_environment_loaded():
            return
        
        try:
            from ..export.clickhouse_service import ClickHouseService
            ch_service = ClickHouseService()
            
            tables = [
                'blocks', 'sync_aggregates', 'execution_payloads', 'transactions',
                'withdrawals', 'attestations', 'deposits', 'voluntary_exits',
                'proposer_slashings', 'attester_slashings', 'bls_changes',
                'blob_commitments', 'execution_requests', 'era_processing_state'
            ]
            
            print("🔧 Optimizing ClickHouse tables...")
            for table in tables:
                try:
                    ch_service.client.command(f"OPTIMIZE TABLE {ch_service.database}.{table} FINAL")
                    print(f"   ✅ Optimized {table}")
                except Exception as e:
                    print(f"   ⚠️  Failed to optimize {table}: {e}")
            
            print("✅ Table optimization completed")
            
        except Exception as e:
            self.handle_error(e, "optimizing tables")