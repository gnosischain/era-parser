import pandas as pd
from typing import List, Dict, Any
from datetime import datetime, timezone

from .base import BaseExporter

class ParquetExporter(BaseExporter):
    """Exporter for Parquet format"""
    
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export blocks to Parquet format (flattened, no sync aggregate fields)"""
        if not blocks:
            print("No blocks to export")
            return
        
        # Flatten blocks for tabular format
        flattened_blocks = [self.flatten_block_for_table(block) for block in blocks]
        df = pd.DataFrame(flattened_blocks)
        
        self._save_parquet_with_metadata(df, output_file, "blocks")
    
    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type to Parquet format"""
        if not data:
            print(f"No {data_type} data to export")
            return
        
        df = pd.DataFrame(data)
        self._save_parquet_with_metadata(df, output_file, data_type)
    
    def _save_parquet_with_metadata(self, df: pd.DataFrame, output_file: str, data_type: str):
        """Save DataFrame to Parquet with metadata"""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            # Convert DataFrame to PyArrow Table
            table = pa.Table.from_pandas(df)
            
            # Create metadata dictionary
            metadata_dict = {
                "era_number": str(self.era_info.get("era_number", "")),
                "start_slot": str(self.era_info.get("start_slot", "")),
                "end_slot": str(self.era_info.get("end_slot", "")),
                "network": self.era_info.get("network", ""),
                "data_type": data_type,
                "export_timestamp": datetime.now(timezone.utc).isoformat(),
                "record_count": str(len(df))
            }
            
            # Add metadata to table schema
            existing_metadata = table.schema.metadata or {}
            existing_metadata.update({k.encode(): v.encode() for k, v in metadata_dict.items()})
            
            # Create new schema with metadata
            new_schema = table.schema.with_metadata(existing_metadata)
            table = table.cast(new_schema)
            
            # Write to parquet
            pq.write_table(table, f"output/{output_file}")
            
        except ImportError:
            # Fallback to basic pandas export without metadata
            print("Warning: PyArrow not available, saving without metadata")
            df.to_parquet(f"output/{output_file}", index=False)
        except Exception as e:
            # Fallback to basic pandas export if metadata fails
            print(f"Warning: Could not add metadata ({e}), saving without metadata")
            df.to_parquet(f"output/{output_file}", index=False)
    
    def export_separate_files(self, all_data: Dict[str, List], base_output: str):
        """Export each data type to separate Parquet files"""
        base_name = base_output.rsplit('.', 1)[0] if '.' in base_output else base_output
        files_created = []
        
        for data_type, data_list in all_data.items():
            if not data_list:  # Skip empty data types
                continue
                
            filename = f"{base_name}_{data_type}.parquet"
            self.export_data_type(data_list, filename, data_type)
            files_created.append(filename)
            print(f"📝 Exported {len(data_list):,} {data_type} records to {filename}")
        
        # Create summary file
        summary_file = f"{base_name}_SUMMARY.txt"
        with open(f"output/{summary_file}", 'w') as f:
            f.write(f"SEPARATE PARQUET FILES EXPORT SUMMARY\n")
            f.write(f"=====================================\n\n")
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