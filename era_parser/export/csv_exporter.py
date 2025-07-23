"""CSV exporter"""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone

from .base import BaseExporter

class CSVExporter(BaseExporter):
    """Exporter for CSV format"""
    
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export blocks to CSV format (flattened)"""
        if not blocks:
            print("No blocks to export")
            return
        
        # Flatten blocks for tabular format
        flattened_blocks = [self.flatten_block_for_table(block) for block in blocks]
        df = pd.DataFrame(flattened_blocks)
        
        # Write with metadata comments
        with open(f"output/{output_file}", 'w') as f:
            f.write(f"# Era {self.era_info['era_number']}: blocks data\n")
            f.write(f"# Slots: {self.era_info['start_slot']} - {self.era_info['end_slot']}\n")
            f.write(f"# Network: {self.era_info['network']}\n")
            f.write(f"# Export timestamp: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"# Total records: {len(blocks)}\n")
            df.to_csv(f, index=False)
    
    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to CSV format"""
        if not data:
            print(f"No {data_type} data to export")
            return
        
        df = pd.DataFrame(data)
        
        # Write with metadata comments
        with open(f"output/{output_file}", 'w') as f:
            f.write(f"# Era {self.era_info['era_number']}: {data_type} data\n")
            f.write(f"# Network: {self.era_info['network']}\n")
            f.write(f"# Export timestamp: {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"# Total records: {len(data)}\n")
            df.to_csv(f, index=False)
    
    def export_separate_files(self, all_data: Dict[str, List], base_output: str):
        """Export each data type to separate CSV files"""
        base_name = base_output.rsplit('.', 1)[0] if '.' in base_output else base_output
        files_created = []
        
        for data_type, data_list in all_data.items():
            if not data_list:  # Skip empty data types
                continue
                
            filename = f"{base_name}_{data_type}.csv"
            self.export_data_type(data_list, filename, data_type)
            files_created.append(filename)
            print(f"📝 Exported {len(data_list):,} {data_type} records to {filename}")
        
        # Create summary file
        summary_file = f"{base_name}_SUMMARY.txt"
        with open(f"output/{summary_file}", 'w') as f:
            f.write(f"SEPARATE CSV FILES EXPORT SUMMARY\n")
            f.write(f"=================================\n\n")
            f.write(f"Era: {self.era_info['era_number']}\n")
            f.write(f"Slots: {self.era_info['start_slot']} - {self.era_info['end_slot']}\n")
            f.write(f"Network: {self.era_info['network']}\n")
            f.write(f"Export timestamp: {datetime.now(timezone.utc).isoformat()}\n\n")
            f.write(f"FILES CREATED:\n")
            for filename in files_created:
                data_type = filename.split('_')[-1].split('.')[0]
                count = len(all_data.get(data_type, []))
                f.write(f"  {filename:<40} ({count:,} records)\n")
        
        files_created.append(summary_file)
        print(f"📊 Summary saved to: {summary_file}")
        
        return files_created