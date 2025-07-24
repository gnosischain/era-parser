"""Batch processing commands"""

import os
from typing import List

from .base import BaseCommand
from ..core import EraProcessor, OutputManager

class BatchCommand(BaseCommand):
    """Handler for batch processing operations"""
    
    def execute(self, args: List[str]) -> None:
        """Execute batch processing command"""
        if not self.validate_required_args(args, 3, "era-parser --batch <pattern> <command> <base_output> [--separate] [--export clickhouse]"):
            return
        
        pattern = args[0]
        command = args[1]
        base_output = args[2]
        
        # Parse flags
        flags, _ = self.parse_flags(args[3:])
        separate_files = flags['separate']
        export_type = self.get_export_type(flags)
        
        # Find era files
        output_manager = OutputManager()
        era_files = output_manager.find_era_files(pattern)
        
        if not era_files:
            print(f"❌ No era files found matching pattern: {pattern}")
            return
        
        print(f"🔍 Found {len(era_files)} era files to process")
        
        # Process each era file
        processed_count = 0
        failed_count = 0
        
        for i, era_file in enumerate(era_files, 1):
            print(f"\n{'='*60}")
            print(f"📈 Processing era file {i}/{len(era_files)}: {os.path.basename(era_file)}")
            print(f"{'='*60}")
            
            try:
                processor = EraProcessor()
                processor.setup(era_file)
                
                # Generate output filename
                if export_type == "file":
                    era_info = processor.era_reader.get_era_info()
                    era_number = era_info.get('era_number', i)
                    output_file = output_manager.generate_batch_output_filename(base_output, era_number)
                    print(f"   📂 Output: {output_file}")
                else:
                    output_file = "clickhouse_output"
                    print(f"   🗄️  Output: ClickHouse")
                
                # Process the era
                success = processor.process_single_era(command, output_file, separate_files, export_type)
                
                if success:
                    processed_count += 1
                    print(f"✅ Successfully processed {os.path.basename(era_file)}")
                else:
                    failed_count += 1
                    print(f"❌ Failed to process {os.path.basename(era_file)}")
                    
            except Exception as e:
                failed_count += 1
                print(f"❌ Error processing {os.path.basename(era_file)}: {e}")
        
        # Final summary
        print(f"\n{'='*60}")
        print(f"🎉 BATCH PROCESSING COMPLETE!")
        print(f"{'='*60}")
        print(f"✅ Successfully processed: {processed_count}/{len(era_files)} files")
        print(f"❌ Failed: {failed_count} files")