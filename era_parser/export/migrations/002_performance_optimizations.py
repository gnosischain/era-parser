"""
Migration 004: Simplified era completion tracking
"""

from .base_migration import BaseMigration

def up(client, database: str):
    """Create simplified era completion tracking"""
    BaseMigration.execute_sql_file(client, database, '004_performance_optimizations_up.sql')

def down(client, database: str):
    """Drop simplified era completion tracking"""
    client.command(f"DROP VIEW IF EXISTS {database}.era_status")
    client.command(f"DROP TABLE IF EXISTS {database}.era_completion")