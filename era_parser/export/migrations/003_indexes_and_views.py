"""
Migration 002: Era processing state management tables
"""

from .base_migration import BaseMigration

def up(client, database: str):
    """Create era state management tables"""
    BaseMigration.execute_sql_file(client, database, '002_era_state_tables_up.sql')

def down(client, database: str):
    """Drop era state management tables"""
    BaseMigration.execute_sql_file(client, database, '002_era_state_tables_down.sql')