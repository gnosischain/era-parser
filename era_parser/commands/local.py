"""Local file processing commands"""

import json
from typing import List

from .base import BaseCommand
from ..core import EraProcessor

class LocalCommand(BaseCommand):
    """Handler for local era file processing commands"""
    
    def execute(self, args: List[str]) -> None:
        """Execute local file processing command"""
        if not self.validate_required_args(args, 2, "era-parser <era_file> <command> [options]"):
            return
        
        era_file = args[0]
        command = args[1]
        
        if not self.validate_file_exists(era_file):
            return
        
        # Parse flags
        flags, clean_args = self.parse_flags(args[2:])
        export_type = self.get_export_type(flags)
        
        # Initialize processor
        processor = EraProcessor()
        processor.setup(era_file)
        
        try:
            if command == "stats":
                self._handle_stats(processor)
                
            elif command == "block":
                self._handle_single_block(processor, clean_args)
                
            elif command == "all-blocks":
                self._handle_all_blocks(processor, clean_args, flags, export_type)
                
            elif command in ["transactions", "withdrawals", "attestations", "sync_aggregates"]:
                self._handle_specific_data_type(processor, command, clean_args, export_type)
                
            else:
                print(f"❌ Unknown command: {command}")
                print("Available commands: block, all-blocks, transactions, withdrawals, attestations, sync_aggregates, stats")
                return
                
            self.print_success("Operation completed successfully!")
            
        except Exception as e:
            self.handle_error(e, f"processing {command}")
    
    def _handle_stats(self, processor: EraProcessor) -> None:
        """Handle stats command"""
        processor.show_stats()
    
    def _handle_single_block(self, processor: EraProcessor, args: List[str]) -> None:
        """Handle single block parsing"""
        if not self.validate_required_args(args, 1, "era-parser <era_file> block <slot>"):
            return
        
        try:
            slot = int(args[0])
            result = processor.parse_single_block(slot)
            print(json.dumps(result, indent=2))
        except ValueError:
            print("❌ Slot must be a valid integer")
    
    def _handle_all_blocks(self, processor: EraProcessor, args: List[str], 
                          flags: dict, export_type: str) -> None:
        """Handle all-blocks command"""
        if export_type == "file" and not args:
            print("❌ Output file required for file export")
            print("Usage: era-parser <era_file> all-blocks <output_file> [--separate] [--export clickhouse]")
            return
        
        output_file = args[0] if export_type == "file" else "clickhouse_output"
        separate_files = flags['separate']
        
        if separate_files or export_type == "clickhouse":
            all_data = processor.extract_all_data()
            processor.export_data(all_data, output_file, "all", separate_files=True, export_type=export_type)
        else:
            blocks = processor.parse_all_blocks()
            processor.export_data(blocks, output_file, "blocks", export_type=export_type)
    
    def _handle_specific_data_type(self, processor: EraProcessor, data_type: str, 
                                 args: List[str], export_type: str) -> None:
        """Handle specific data type extraction"""
        if export_type == "file" and not args:
            print(f"❌ Output file required for file export")
            print(f"Usage: era-parser <era_file> {data_type} <output_file> [--export clickhouse]")
            return
        
        output_file = args[0] if export_type == "file" else "clickhouse_output"
        data = processor.extract_specific_data(data_type)
        processor.export_data(data, output_file, data_type, export_type=export_type)