from .base_migration import BaseMigration

def up(client, database: str):
    """Create initial beacon chain tables"""
    BaseMigration.execute_sql_file(client, database, '001_initial_tables_up.sql')

def down(client, database: str):
    """Drop initial beacon chain tables"""
    BaseMigration.execute_sql_file(client, database, '001_initial_tables_down.sql')