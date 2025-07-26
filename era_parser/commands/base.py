"""Base command class with common functionality"""

import os
import sys
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional

class BaseCommand(ABC):
    """Base class for all CLI commands"""
    
    def __init__(self):
        """Initialize base command"""
        self.debug = False
        
    @abstractmethod
    def execute(self, args: List[str]) -> None:
        """Execute the command with given arguments"""
        pass
    
    def setup_output_directory(self) -> None:
        """Ensure output directory exists"""
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def parse_flags(self, args: List[str]) -> tuple:
        """Parse common flags from arguments"""
        flags = {
            'separate': '--separate' in args,
            'download_only': '--download-only' in args,
            'export_clickhouse': '--export' in args and 'clickhouse' in args
        }
        
        # Remove flags from args for cleaner processing
        clean_args = [arg for arg in args if not arg.startswith('--')]
        
        return flags, clean_args
        
    def get_export_type(self, flags: Dict[str, Any]) -> str:
        """Determine export type from flags"""
        return "clickhouse" if flags['export_clickhouse'] else "file"
    
    def handle_error(self, error: Exception, context: str = "") -> None:
        """Handle and display errors consistently"""
        if context:
            print(f"❌ Error in {context}: {error}")
        else:
            print(f"❌ Error: {error}")
        
        if self.debug:
            import traceback
            traceback.print_exc()
    
    def print_success(self, message: str) -> None:
        """Print success message"""
        print(f"✅ {message}")
    
    def print_info(self, message: str) -> None:
        """Print info message"""
        print(f"ℹ️  {message}")
    
    def print_warning(self, message: str) -> None:
        """Print warning message"""
        print(f"⚠️  {message}")
    
    def validate_file_exists(self, filepath: str) -> bool:
        """Validate that a file exists"""
        if not os.path.exists(filepath):
            print(f"❌ File not found: {filepath}")
            return False
        return True
    
    def validate_required_args(self, args: List[str], min_count: int, usage: str) -> bool:
        """Validate minimum argument count"""
        if len(args) < min_count:
            print(f"❌ Insufficient arguments")
            print(f"Usage: {usage}")
            return False
        return True