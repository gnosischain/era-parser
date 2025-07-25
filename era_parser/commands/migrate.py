import sys
from typing import List
from ..export.migrations import MigrationManager
from ..export.clickhouse_service import ClickHouseService
from ..ingestion.remote_downloader import load_env_file 

class MigrateCommand:
    """Command for managing ClickHouse migrations"""
    
    def execute(self, args: List[str]):
        """Execute migration command"""
        # Load .env file using existing function
        load_env_file()
        
        if not args:
            self._show_help()
            return
        
        subcommand = args[0]
        
        if subcommand == "status":
            self._show_status()
        elif subcommand == "run":
            target_version = args[1] if len(args) > 1 else None
            self._run_migrations(target_version)
        elif subcommand == "list":
            self._list_migrations()
        else:
            print(f"❌ Unknown migration subcommand: {subcommand}")
            self._show_help()
    
    def _show_help(self):
        """Show migration help"""
        print("Era Parser - ClickHouse Migration Commands")
        print("")
        print("USAGE:")
        print("  era-parser --migrate status                  # Show migration status")
        print("  era-parser --migrate run [version]           # Run migrations (to version)")
        print("  era-parser --migrate list                    # List available migrations")
        print("")
        print("EXAMPLES:")
        print("  era-parser --migrate status                  # Check current status")
        print("  era-parser --migrate run                     # Run all pending migrations")
        print("  era-parser --migrate run 002                 # Run migrations up to version 002")
        print("")
    
    def _show_status(self):
        """Show migration status"""
        try:
            service = ClickHouseService()
            status = service.get_migration_status()
            
            print("📊 MIGRATION STATUS")
            print("=" * 50)
            print(f"Applied migrations: {status['applied_count']}")
            print(f"Available migrations: {status['available_count']}")
            print(f"Pending migrations: {status['pending_count']}")
            print(f"Last applied: {status['last_applied'] or 'None'}")
            
            if status['pending_versions']:
                print(f"Pending versions: {', '.join(status['pending_versions'])}")
            
            if 'error' in status:
                print(f"⚠️  Error: {status['error']}")
                
        except Exception as e:
            print(f"❌ Failed to get migration status: {e}")
    
    def _run_migrations(self, target_version: str = None):
        """Run migrations"""
        try:
            service = ClickHouseService()
            
            if target_version:
                print(f"🚀 Running migrations up to version {target_version}")
            else:
                print("🚀 Running all pending migrations")
            
            success = service.run_migrations(target_version)
            
            if success:
                print("✅ Migrations completed successfully")
                self._show_status()
            else:
                print("❌ Migration failed")
                sys.exit(1)
                
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            sys.exit(1)
    
    def _list_migrations(self):
        """List available migrations"""
        try:
            from ..export.clickhouse_service import ClickHouseService
            service = ClickHouseService()
            
            migration_manager = MigrationManager(service.client, service.database)
            available = migration_manager.get_available_migrations()
            applied = set(migration_manager.get_applied_migrations())
            
            print("📋 AVAILABLE MIGRATIONS")
            print("=" * 50)
            
            for migration in available:
                status = "✅ Applied" if migration['version'] in applied else "⏳ Pending"
                print(f"{migration['version']}: {migration['name']} - {status}")
                
        except Exception as e:
            print(f"❌ Failed to list migrations: {e}")