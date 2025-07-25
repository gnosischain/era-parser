"""
ClickHouse Migration Manager
Simple migration system that maintains backward compatibility
"""

import os
import logging
import importlib
from typing import List, Dict, Any, Optional
from datetime import datetime
import clickhouse_connect

logger = logging.getLogger(__name__)

class MigrationManager:
    """Manages ClickHouse schema migrations with backward compatibility"""
    
    def __init__(self, client, database: str):
        """
        Initialize migration manager
        
        Args:
            client: ClickHouse client instance
            database: Database name
        """
        self.client = client
        self.database = database
        self.migrations_dir = os.path.dirname(__file__)
        
    def ensure_migration_table(self):
        """Create migration tracking table if it doesn't exist"""
        try:
            self.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.database}.schema_migrations (
                version String,
                name String,
                applied_at DateTime DEFAULT now(),
                checksum String DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY version
            """)
            logger.debug("Migration tracking table ensured")
        except Exception as e:
            logger.error(f"Failed to create migration table: {e}")
            raise
    
    def get_applied_migrations(self) -> List[str]:
        """Get list of applied migration versions"""
        try:
            result = self.client.query(f"""
            SELECT version FROM {self.database}.schema_migrations 
            ORDER BY version
            """)
            return [row[0] for row in result.result_rows]
        except Exception as e:
            logger.debug(f"Could not get applied migrations (table may not exist): {e}")
            return []
    
    def get_available_migrations(self) -> List[Dict[str, str]]:
        """Get list of available migration files"""
        migrations = []
        
        for filename in sorted(os.listdir(self.migrations_dir)):
            if filename.endswith('.py') and filename[0].isdigit():
                # Extract version from filename (e.g., "001_initial_tables.py")
                version = filename.split('_')[0]
                name = filename[:-3]  # Remove .py extension
                migrations.append({
                    'version': version,
                    'name': name,
                    'filename': filename
                })
        
        return migrations
    
    def run_migrations(self, target_version: Optional[str] = None) -> bool:
        """
        Run pending migrations up to target version
        
        Args:
            target_version: Version to migrate to (None = latest)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure migration table exists
            self.ensure_migration_table()
            
            # Get applied and available migrations
            applied = set(self.get_applied_migrations())
            available = self.get_available_migrations()
            
            # Filter to target version if specified
            if target_version:
                available = [m for m in available if m['version'] <= target_version]
            
            # Find pending migrations
            pending = [m for m in available if m['version'] not in applied]
            
            if not pending:
                logger.info("No pending migrations found")
                return True
            
            logger.info(f"Running {len(pending)} pending migrations")
            
            # Run each pending migration
            for migration in pending:
                if not self._run_single_migration(migration):
                    logger.error(f"Migration {migration['version']} failed, stopping")
                    return False
            
            logger.info("All migrations completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Migration process failed: {e}")
            return False
    
    def _run_single_migration(self, migration: Dict[str, str]) -> bool:
        """Run a single migration file"""
        try:
            version = migration['version']
            name = migration['name']
            
            logger.info(f"Running migration {version}: {name}")
            
            # Try SQL file first, then Python fallback
            sql_executed = self._try_sql_migration(version, name, 'up')
            
            if not sql_executed:
                # Fallback to Python migration
                success = self._try_python_migration(name, 'up')
                if not success:
                    return False
            
            # Record successful migration
            self._record_migration(version, name)
            
            logger.info(f"Migration {version} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to run migration {migration['version']}: {e}")
            return False
    
    def _try_sql_migration(self, version: str, name: str, direction: str) -> bool:
        """Try to run SQL migration file"""
        try:
            sql_filename = f"{version}_{name.split('_', 1)[1]}_{direction}.sql"
            sql_path = os.path.join(self.migrations_dir, 'sql', sql_filename)
            
            if not os.path.exists(sql_path):
                logger.debug(f"SQL file not found: {sql_path}")
                return False
            
            # Read and execute SQL file
            with open(sql_path, 'r') as f:
                sql_content = f.read()
            
            # Replace {database} placeholder
            sql_content = sql_content.format(database=self.database)
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for statement in statements:
                logger.debug(f"Executing SQL: {statement[:100]}...")
                self.client.command(statement)
            
            logger.info(f"SQL migration {sql_filename} executed successfully")
            return True
            
        except Exception as e:
            logger.warning(f"SQL migration failed: {e}")
            return False
    
    def _try_python_migration(self, name: str, direction: str) -> bool:
        """Try to run Python migration as fallback"""
        try:
            # Import the migration module
            module_name = f"era_parser.export.migrations.{name}"
            module = importlib.import_module(module_name)
            
            # Check if migration has the required function
            if not hasattr(module, direction):
                logger.error(f"Migration {name} missing '{direction}' function")
                return False
            
            # Run the migration
            getattr(module, direction)(self.client, self.database)
            
            logger.info(f"Python migration {name} executed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Python migration failed: {e}")
            return False
    
    def _record_migration(self, version: str, name: str):
        """Record successful migration in tracking table"""
        try:
            self.client.insert(
                f'{self.database}.schema_migrations',
                [[version, name, '']],
                column_names=['version', 'name', 'checksum']
            )
        except Exception as e:
            logger.error(f"Failed to record migration {version}: {e}")
            raise
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status"""
        try:
            applied = self.get_applied_migrations()
            available = self.get_available_migrations()
            
            pending_versions = []
            for migration in available:
                if migration['version'] not in applied:
                    pending_versions.append(migration['version'])
            
            return {
                'applied_count': len(applied),
                'available_count': len(available),
                'pending_count': len(pending_versions),
                'last_applied': max(applied) if applied else None,
                'pending_versions': pending_versions
            }
            
        except Exception as e:
            logger.error(f"Failed to get migration status: {e}")
            return {
                'applied_count': 0,
                'available_count': 0,
                'pending_count': 0,
                'last_applied': None,
                'pending_versions': []
            }