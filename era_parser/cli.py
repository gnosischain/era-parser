import sys
import os
import json
import glob
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from .ingestion import EraReader
from .parsing import BlockParser
from .export import JSONExporter, CSVExporter, ParquetExporter
from .config import detect_network_from_filename
from .ingestion.remote_downloader import RemoteEraDownloader, get_remote_era_downloader

class EraParserCLI:
    """Enhanced command line interface with remote era file support"""
    
    def __init__(self):
        self.network = None
        self.era_reader = None
        self.block_parser = None
    
    def setup(self, era_file: str, network: str = None):
        """Setup CLI with era file"""
        self.network = network or detect_network_from_filename(era_file)
        self.era_reader = EraReader(era_file, self.network)
        self.block_parser = BlockParser(self.network)
    
    def parse_era_range(self, era_range: str) -> Tuple[int, Optional[int]]:
        """
        Parse era range string into start and end values
        
        Args:
            era_range: Range string like "1082", "1082-1100", "1082+"
            
        Returns:
            Tuple of (start_era, end_era) where end_era is None for open ranges
        """
        if '+' in era_range:
            # Open range: "1082+"
            start_era = int(era_range.replace('+', ''))
            return start_era, None
        elif '-' in era_range:
            # Closed range: "1082-1100"
            start_str, end_str = era_range.split('-', 1)
            return int(start_str), int(end_str)
        else:
            # Single era: "1082"
            era = int(era_range)
            return era, era
    
    def process_remote_eras(self, network: str, era_range: str, command: str, 
                           base_output: str, separate_files: bool = False,
                           download_only: bool = False, resume: bool = False) -> Dict[str, Any]:
        """
        Process remote era files
        
        Args:
            network: Network name (gnosis, mainnet, sepolia)
            era_range: Era range string (e.g., "1082-1100", "1082+")
            command: Processing command
            base_output: Base output filename
            separate_files: Whether to create separate files per data type
            download_only: Only download, don't process
            resume: Resume from previous run
            
        Returns:
            Processing summary
        """
        print(f"🌐 Remote Era Processing")
        print(f"   Network: {network}")
        print(f"   Era range: {era_range}")
        print(f"   Command: {command}")
        
        # Parse era range
        start_era, end_era = self.parse_era_range(era_range)
        
        # Get downloader from environment
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network  # Set the network
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            print("💡 Make sure to set ERA_BASE_URL environment variable")
            print("   Example: export ERA_BASE_URL=https://era-files-dadb9c4ad1d99b9f.s3.eu-central-1.amazonaws.com")
            return {"success": False, "error": str(e)}
        
        if download_only:
            print("📥 Download-only mode")
            # Discover and download files
            available_eras = downloader.discover_era_files(start_era, end_era)
            
            downloaded_count = 0
            for era_number, url in available_eras:
                local_path = downloader.download_era(era_number, url)
                if local_path:
                    downloaded_count += 1
                    print(f"✅ Downloaded era {era_number} to {local_path}")
                else:
                    print(f"❌ Failed to download era {era_number}")
            
            return {
                "success": True,
                "mode": "download_only",
                "downloaded_count": downloaded_count,
                "total_available": len(available_eras)
            }
        else:
            # Download and process
            return downloader.process_era_range(
                start_era=start_era,
                end_era=end_era,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume
            )
    
    def show_remote_progress(self, network: str) -> Dict[str, Any]:
        """Show progress for remote processing"""
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
            progress = downloader.list_progress()
            
            print(f"📊 Remote Processing Progress ({network})")
            print(f"   Processed eras: {progress['processed_eras']}")
            print(f"   Failed eras: {progress['failed_eras']}")
            if progress['last_run']:
                import datetime
                last_run = datetime.datetime.fromtimestamp(progress['last_run'])
                print(f"   Last run: {last_run.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Progress file: {progress['progress_file']}")
            
            return progress
            
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            return {"success": False, "error": str(e)}
    
    def clear_remote_progress(self, network: str):
        """Clear remote processing progress"""
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
            downloader.clear_progress()
            print(f"✅ Cleared progress for {network}")
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
    
    # [Keep all existing methods from the original implementation]
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
    
    def show_stats(self):
        """Show era file statistics"""
        era_info = self.era_reader.get_era_info()
        stats = self.era_reader.get_statistics()
        
        print(f"📊 Era File Statistics: {os.path.basename(self.era_reader.filepath)}")
        print(f"   Network: {era_info['network']}")
        print(f"   Era Number: {era_info['era_number']}")
        print(f"   Slot Range: {era_info['start_slot']} - {era_info['end_slot']}")
        print(f"   Hash: {era_info['hash']}")
        print(f"   Total Records: {stats['total_records']}")
        print(f"   Blocks: {stats['blocks']}")
        print(f"   States: {stats['states']}")  
        print(f"   Indices: {stats['indices']}")
        
        if stats.get('min_slot') and stats.get('max_slot'):
            print(f"   Block Slot Range: {stats['min_slot']} - {stats['max_slot']}")
    
    def parse_single_block(self, slot: int) -> Dict[str, Any]:
        """Parse a single block by slot"""
        block_records = self.era_reader.get_block_records()
        
        for record_slot, compressed_data in block_records:
            if record_slot == slot:
                return self.block_parser.parse_block(compressed_data, slot)
        
        return {'error': f'Block with slot {slot} not found in era file'}
    
    def parse_all_blocks(self) -> List[Dict[str, Any]]:
        """Parse all blocks in the era"""
        block_records = self.era_reader.get_block_records()
        blocks = []
        successful = 0
        
        print(f"🔍 Found {len(block_records)} blocks to process")
        
        for i, (slot, compressed_data) in enumerate(block_records):
            if (i + 1) % 100 == 0:
                print(f"📈 Processing block {i + 1}/{len(block_records)} (slot {slot})")
            
            block = self.block_parser.parse_block(compressed_data, slot)
            if block:
                blocks.append(block)
                successful += 1
            else:
                print(f"⚠️  Failed to parse block at slot {slot}")
        
        print(f"✅ Successfully processed {successful}/{len(block_records)} blocks")
        return blocks
    
    def _process_single_era(self, command: str, output_file: str, separate_files: bool) -> bool:
        """Process a single era file"""
        try:
            print(f"   🔧 Processing with command: '{command}'")
            print(f"   📁 Output file: '{output_file}'")
            print(f"   🔀 Separate files: {separate_files}")
            
            if command == "all-blocks":
                if separate_files:
                    all_data = self.extract_all_data_types()
                    self.export_data(all_data, output_file, "all", separate_files=True)
                else:
                    blocks = self.parse_all_blocks()
                    self.export_data(blocks, output_file, "blocks")
                    
            elif command in ["transactions", "withdrawals", "attestations"]:
                data = self.extract_specific_data(command)
                self.export_data(data, output_file, command)
            else:
                print(f"❌ Unknown command for processing: '{command}'")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ Error during processing: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def extract_specific_data(self, data_type: str) -> List[Dict[str, Any]]:
        """Extract specific data type from all blocks"""
        block_records = self.era_reader.get_block_records()
        data = []
        successful = 0
        
        print(f"🎯 Extracting {data_type} data from {len(block_records)} blocks...")
        
        for i, (slot, compressed_data) in enumerate(block_records):
            if (i + 1) % 100 == 0:
                print(f"📈 Processing block {i + 1}/{len(block_records)} (slot {slot})")
            
            block = self.block_parser.parse_block(compressed_data, slot)
            if not block:
                continue
                
            successful += 1
            extracted = self._extract_data_from_block(block, data_type, slot)
            data.extend(extracted)
        
        print(f"✅ Extracted {len(data)} {data_type} records from {successful} blocks")
        return data
    
    def extract_all_data_types(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract all data types from blocks"""
        block_records = self.era_reader.get_block_records()
        
        all_data = {
            'blocks': [],
            'transactions': [],
            'withdrawals': [],
            'attestations': [],
            'deposits': [],
            'voluntary_exits': [],
            'proposer_slashings': [],
            'attester_slashings': [],
            'bls_changes': [],
            'blob_commitments': [],
            'execution_requests': []
        }
        
        successful = 0
        
        print("🌍 COMPREHENSIVE EXPORT MODE - Extracting all data types...")
        
        for i, (slot, compressed_data) in enumerate(block_records):
            if (i + 1) % 100 == 0:
                print(f"📈 Processing block {i + 1}/{len(block_records)} (slot {slot})")
            
            block = self.block_parser.parse_block(compressed_data, slot)
            if not block:
                continue
                
            successful += 1
            message = block.get("data", {}).get("message", {})
            body = message.get("body", {})
            execution_payload = body.get("execution_payload", {})
            
            # Block summary
            all_data['blocks'].append({
                "slot": slot,
                "proposer_index": message.get("proposer_index"),
                "parent_root": message.get("parent_root"),
                "state_root": message.get("state_root"),
                "version": block.get("version"),
                "timestamp_utc": block.get("timestamp_utc"),
                "signature": block.get("data", {}).get("signature"),
                "block_hash": execution_payload.get("block_hash"),
                "fee_recipient": execution_payload.get("fee_recipient"),
                "gas_used": execution_payload.get("gas_used"),
                "transaction_count": len(execution_payload.get("transactions", [])),
                "withdrawal_count": len(execution_payload.get("withdrawals", [])),
                "attestation_count": len(body.get("attestations", [])),
            })
            
            # Extract specific data types
            all_data['transactions'].extend(self._extract_data_from_block(block, "transactions", slot))
            all_data['withdrawals'].extend(self._extract_data_from_block(block, "withdrawals", slot))
            all_data['attestations'].extend(self._extract_data_from_block(block, "attestations", slot))
            
            # Add other data types with simplified extraction
            for deposit_idx, deposit in enumerate(body.get("deposits", [])):
                all_data['deposits'].append({
                    "slot": slot,
                    "deposit_index": deposit_idx,
                    "deposit_data": json.dumps(deposit) if deposit else None,
                })
        
        print(f"✅ Successfully processed {successful} blocks")
        return all_data
    
    def _extract_data_from_block(self, block: Dict[str, Any], data_type: str, slot: int) -> List[Dict[str, Any]]:
        """Extract specific data type from a single block"""
        message = block.get("data", {}).get("message", {})
        body = message.get("body", {})
        execution_payload = body.get("execution_payload", {})
        
        extracted = []
        
        if data_type == "transactions":
            transactions = execution_payload.get("transactions", [])
            for tx_index, tx_hash in enumerate(transactions):
                extracted.append({
                    "slot": slot,
                    "block_number": execution_payload.get("block_number"),
                    "block_hash": execution_payload.get("block_hash"),
                    "transaction_index": tx_index,
                    "transaction_hash": tx_hash,
                    "fee_recipient": execution_payload.get("fee_recipient"),
                    "gas_limit": execution_payload.get("gas_limit"),
                    "gas_used": execution_payload.get("gas_used"),
                    "base_fee_per_gas": execution_payload.get("base_fee_per_gas"),
                    "timestamp": execution_payload.get("timestamp"),
                })
                
        elif data_type == "withdrawals":
            withdrawals = execution_payload.get("withdrawals", [])
            for withdrawal in withdrawals:
                extracted.append({
                    "slot": slot,
                    "block_number": execution_payload.get("block_number"),
                    "block_hash": execution_payload.get("block_hash"),
                    "withdrawal_index": withdrawal.get("index"),
                    "validator_index": withdrawal.get("validator_index"),
                    "address": withdrawal.get("address"),
                    "amount": withdrawal.get("amount"),
                    "timestamp": execution_payload.get("timestamp"),
                })
                
        elif data_type == "attestations":
            attestations = body.get("attestations", [])
            for att_index, attestation in enumerate(attestations):
                att_data = attestation.get("data", {})
                extracted.append({
                    "slot": slot,
                    "attestation_index": att_index,
                    "attestation_slot": att_data.get("slot"),
                    "committee_index": att_data.get("index"),
                    "beacon_block_root": att_data.get("beacon_block_root"),
                    "source_epoch": att_data.get("source", {}).get("epoch"),
                    "source_root": att_data.get("source", {}).get("root"),
                    "target_epoch": att_data.get("target", {}).get("epoch"),
                    "target_root": att_data.get("target", {}).get("root"),
                    "aggregation_bits": attestation.get("aggregation_bits"),
                    "signature": attestation.get("signature"),
                })
        
        return extracted
    
    def export_data(self, data, output_file: str, data_type: str = "blocks", separate_files: bool = False):
        """Export data using appropriate exporter"""
        era_info = self.era_reader.get_era_info()
        
        # Determine exporter based on file extension
        if output_file.endswith(('.json', '.jsonl')):
            exporter = JSONExporter(era_info)
            if data_type == "blocks":
                exporter.export_blocks(data, output_file)
            else:
                exporter.export_data_type(data, output_file, data_type)
                
        elif output_file.endswith('.csv'):
            exporter = CSVExporter(era_info)
            if separate_files and isinstance(data, dict):
                exporter.export_separate_files(data, output_file)
            elif data_type == "blocks":
                exporter.export_blocks(data, output_file)
            else:
                exporter.export_data_type(data, output_file, data_type)
                
        elif output_file.endswith('.parquet'):
            exporter = ParquetExporter(era_info)
            if separate_files and isinstance(data, dict):
                exporter.export_separate_files(data, output_file)
            elif data_type == "blocks":
                exporter.export_blocks(data, output_file)
            else:
                exporter.export_data_type(data, output_file, data_type)
        
        else:
            raise ValueError(f"Unsupported output format: {output_file}")


def main():
    """Enhanced main CLI entry point with remote era file support"""
    if len(sys.argv) < 2:
        print("Era Parser - Ethereum Beacon Chain Era File Parser")
        print("")
        print("LOCAL FILE COMMANDS:")
        print("  era-parser <era_file> block <slot>                    # Parse single block")
        print("  era-parser <era_file> all-blocks <output_file>        # All blocks")
        print("  era-parser <era_file> all-blocks <output_file> --separate  # Separate files")
        print("  era-parser <era_file> transactions <output_file>      # Transaction data only")
        print("  era-parser <era_file> withdrawals <output_file>       # Withdrawal data only")
        print("  era-parser <era_file> attestations <output_file>      # Attestation data only")
        print("  era-parser <era_file> stats                           # Show era statistics")
        print("")
        print("REMOTE ERA PROCESSING:")
        print("  era-parser --remote <network> <era_range> <command> <output>           # Process remote eras")
        print("  era-parser --remote <network> <era_range> <command> <output> --separate  # Separate files")
        print("  era-parser --remote <network> <era_range> --download-only              # Download only")
        print("  era-parser --remote <network> <era_range> <command> <output> --resume # Resume processing")
        print("")
        print("REMOTE UTILITY COMMANDS:")
        print("  era-parser --remote-progress <network>                # Show remote progress")
        print("  era-parser --remote-clear <network>                   # Clear remote progress")
        print("")
        print("ERA RANGE FORMATS:")
        print("  1082        # Single era")
        print("  1082-1100   # Era range (inclusive)")
        print("  1082+       # From era 1082 until no more files found")
        print("")
        print("REMOTE EXAMPLES:")
        print("  # Set base URL first")
        print("  export ERA_BASE_URL=https://era-files-dadb9c4ad1d99b9f.s3.eu-central-1.amazonaws.com")
        print("")
        print("  # Process era range")
        print("  era-parser --remote gnosis 1082-1100 all-blocks gnosis_data.parquet --separate")
        print("")
        print("  # Process from era until end")
        print("  era-parser --remote gnosis 1082+ transactions txs.csv")
        print("")
        print("  # Download only (no processing)")
        print("  era-parser --remote gnosis 1082-1100 --download-only")
        print("")
        print("  # Resume previous processing")
        print("  era-parser --remote gnosis 1082+ all-blocks data.parquet --resume")
        print("")
        print("ENVIRONMENT VARIABLES:")
        print("  ERA_BASE_URL              # Base URL for remote era files (required)")
        print("  ERA_DOWNLOAD_DIR          # Directory for temporary downloads (optional)")
        print("  ERA_CLEANUP_AFTER_PROCESS # Delete files after processing (default: true)")
        print("  ERA_MAX_RETRIES           # Maximum download retries (default: 3)")
        sys.exit(1)
    
    cli = EraParserCLI()
    
    # Handle remote processing commands
    if sys.argv[1] == "--remote":
        if len(sys.argv) < 5:
            print("Usage: era-parser --remote <network> <era_range> <command> <output> [--separate] [--resume]")
            print("   or: era-parser --remote <network> <era_range> --download-only")
            sys.exit(1)
        
        network = sys.argv[2]
        era_range = sys.argv[3]
        
        # Check for download-only mode
        if len(sys.argv) > 4 and sys.argv[4] == "--download-only":
            result = cli.process_remote_eras(
                network=network,
                era_range=era_range,
                command="",
                base_output="",
                download_only=True
            )
            if result["success"]:
                print(f"🎉 Downloaded {result['downloaded_count']}/{result['total_available']} era files")
            sys.exit(0 if result["success"] else 1)
        
        if len(sys.argv) < 6:
            print("Usage: era-parser --remote <network> <era_range> <command> <output> [--separate] [--resume]")
            sys.exit(1)
        
        command = sys.argv[4]
        base_output = sys.argv[5]
        separate_files = "--separate" in sys.argv
        resume = "--resume" in sys.argv
        
        try:
            result = cli.process_remote_eras(
                network=network,
                era_range=era_range,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume
            )
            
            if result["success"]:
                print(f"🎉 Remote processing completed successfully!")
                print(f"   Processed: {result['processed_count']} eras")
                if result['failed_count'] > 0:
                    print(f"   Failed: {result['failed_count']} eras")
            else:
                print(f"❌ Remote processing failed: {result.get('error', 'Unknown error')}")
                sys.exit(1)
                
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Handle remote utility commands
    elif sys.argv[1] == "--remote-progress":
        if len(sys.argv) < 3:
            print("Usage: era-parser --remote-progress <network>")
            sys.exit(1)
        
        network = sys.argv[2]
        cli.show_remote_progress(network)
    
    elif sys.argv[1] == "--remote-clear":
        if len(sys.argv) < 3:
            print("Usage: era-parser --remote-clear <network>")
            sys.exit(1)
        
        network = sys.argv[2]
        cli.clear_remote_progress(network)
    
    # Handle local file processing (existing logic)
    else:
        era_file = sys.argv[1]
        command = sys.argv[2] if len(sys.argv) > 2 else "help"
        
        if not os.path.exists(era_file):
            print(f"❌ Era file not found: {era_file}")
            sys.exit(1)
        
        # Check for --separate flag
        separate_files = "--separate" in sys.argv
        
        # Initialize CLI
        cli.setup(era_file)
        
        try:
            if command == "stats":
                cli.show_stats()
                
            elif command == "block":
                if len(sys.argv) < 4:
                    print("Usage: era-parser <era_file> block <slot>")
                    sys.exit(1)
                slot = int(sys.argv[3])
                result = cli.parse_single_block(slot)
                print(json.dumps(result, indent=2))
                
            elif command == "all-blocks":
                if len(sys.argv) < 4:
                    print("Usage: era-parser <era_file> all-blocks <output_file> [--separate]")
                    sys.exit(1)
                output_file = sys.argv[3]
                
                if separate_files:
                    all_data = cli.extract_all_data_types()
                    cli.export_data(all_data, output_file, "all", separate_files=True)
                else:
                    blocks = cli.parse_all_blocks()
                    cli.export_data(blocks, output_file, "blocks")
                
            elif command in ["transactions", "withdrawals", "attestations"]:
                if len(sys.argv) < 4:
                    print(f"Usage: era-parser <era_file> {command} <output_file>")
                    sys.exit(1)
                output_file = sys.argv[3]
                data = cli.extract_specific_data(command)
                cli.export_data(data, output_file, command)
                
            else:
                print(f"❌ Unknown command: {command}")
                print("Available commands: block, all-blocks, transactions, withdrawals, attestations, stats")
                sys.exit(1)
                
            print(f"🎉 Operation completed successfully!")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    main()