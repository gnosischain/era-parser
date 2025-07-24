"""Output file management utilities"""

import os
import glob
from pathlib import Path
from typing import List

class OutputManager:
    """Manages output file naming and directory operations"""
    
    def __init__(self, base_output_dir: str = "output"):
        """Initialize output manager"""
        self.base_output_dir = base_output_dir
        self.ensure_output_directory()
    
    def ensure_output_directory(self) -> None:
        """Ensure output directory exists"""
        if not os.path.exists(self.base_output_dir):
            os.makedirs(self.base_output_dir)
    
    def generate_era_output_filename(self, base_output: str, era_number: int) -> str:
        """Generate output filename for a specific era"""
        # Extract directory and base name
        output_dir = os.path.dirname(base_output) if os.path.dirname(base_output) else ""
        base_name = os.path.splitext(os.path.basename(base_output))[0]
        extension = os.path.splitext(base_output)[1] or ".json"
        
        # Generate filename with era number
        filename = f"{base_name}_era_{era_number:05d}{extension}"
        
        if output_dir:
            return os.path.join(output_dir, filename)
        else:
            return filename
    
    def generate_batch_output_filename(self, base_output: str, era_number: int) -> str:
        """Generate output filename for batch processing"""
        if base_output.endswith(('.json', '.csv', '.parquet')):
            base_name = base_output.rsplit('.', 1)[0]
            extension = '.' + base_output.rsplit('.', 1)[1]
            return f"{base_name}_era_{era_number:05d}{extension}"
        else:
            return f"{base_output}_era_{era_number:05d}.parquet"
    
    def find_era_files(self, pattern: str) -> List[str]:
        """Find era files matching pattern"""
        era_files = []
        
        if os.path.isdir(pattern):
            pattern = os.path.join(pattern, "*.era")
        
        matches = glob.glob(pattern)
        
        for match in matches:
            if match.endswith('.era') and os.path.isfile(match):
                era_files.append(match)
        
        def extract_era_number(filepath):
            filename = os.path.basename(filepath)
            parts = filename.replace('.era', '').split('-')
            try:
                return int(parts[-2]) if len(parts) >= 2 else 0
            except (ValueError, IndexError):
                return 0
        
        era_files.sort(key=extract_era_number)
        return era_files
    
    def get_output_path(self, filename: str) -> str:
        """Get full output path for a filename"""
        return os.path.join(self.base_output_dir, filename)
    
    def validate_output_format(self, output_file: str) -> bool:
        """Validate that output format is supported"""
        supported_extensions = ['.json', '.jsonl', '.csv', '.parquet']
        extension = os.path.splitext(output_file)[1].lower()
        return extension in supported_extensions
    
    def get_file_size_mb(self, filepath: str) -> float:
        """Get file size in MB"""
        if os.path.exists(filepath):
            return os.path.getsize(filepath) / (1024 * 1024)
        return 0.0