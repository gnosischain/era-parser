import os
import logging

logger = logging.getLogger(__name__)

class BaseMigration:
    """Base class for all SQL-based migrations"""
    
    @staticmethod
    def execute_sql_file(client, database: str, filename: str):
        """Execute SQL file with database placeholder replacement"""
        sql_path = os.path.join(os.path.dirname(__file__), 'sql', filename)
        
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        
        logger.info(f"Executing SQL file: {filename}")
        
        with open(sql_path, 'r') as f:
            sql_content = f.read()
        
        # Replace {database} placeholder
        sql_content = sql_content.format(database=database)
        
        # Split by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        for i, statement in enumerate(statements, 1):
            try:
                client.command(statement)
                logger.debug(f"Statement {i}/{len(statements)} executed successfully")
            except Exception as e:
                logger.error(f"Failed to execute statement {i}: {statement[:100]}...")
                raise
        
        logger.info(f"SQL file {filename} executed successfully ({len(statements)} statements)")