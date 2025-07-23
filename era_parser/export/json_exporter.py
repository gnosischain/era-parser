"""JSON and JSONL exporters"""

import json
from typing import List, Dict, Any

from .base import BaseExporter

class JSONExporter(BaseExporter):
    """Exporter for JSON and JSONL formats"""
    
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export blocks to JSON format"""
        if output_file.endswith('.jsonl'):
            self._export_jsonl(blocks, output_file, "blocks")
        else:
            self._export_json(blocks, output_file, "blocks")
    
    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to JSON format"""
        if output_file.endswith('.jsonl'):
            self._export_jsonl(data, output_file, data_type)
        else:
            self._export_json(data, output_file, data_type)
    
    def _export_json(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export to JSON format"""
        output_data = self.create_metadata(len(data), data_type)
        output_data["data"] = data
        
        with open(f"output/{output_file}", 'w') as f:
            json.dump(output_data, f, indent=2)
    
    def _export_jsonl(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export to JSON Lines format"""
        with open(f"output/{output_file}", 'w') as f:
            # Write metadata as first line
            metadata = self.create_metadata(len(data), data_type)
            metadata["type"] = "metadata"
            f.write(json.dumps(metadata) + '\n')
            
            # Write each item as a separate line
            for item in data:
                f.write(json.dumps(item) + '\n')