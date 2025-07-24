import sys
import os
import json
import glob
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

from .ingestion import EraReader
from .parsing import BlockParser
from .export import JSONExporter, CSVExporter, ParquetExporter, ClickHouseExporter, EraStateManager
from .config import detect_network_from_filename, get_network_config
from .ingestion.remote_downloader import RemoteEraDownloader, get_remote_era_downloader

class EraParserCLI:
    """Simple CLI with full data extraction"""
    
    def __init__(self):
        self.network = None
        self.network_config = None
        self.era_reader = None
        self.block_parser = None
    
    def setup(self, era_file: str, network: str = None):
        """Setup CLI with era file"""
        self.network = network or detect_network_from_filename(era_file)
        self.network_config = get_network_config(self.network)
        self.era_reader = EraReader(era_file, self.network)
        self.block_parser = BlockParser(self.network)
    
    def _calculate_slot_timestamp(self, slot: int) -> str:
        """Calculate timestamp for a slot using network configuration"""
        genesis_time = self.network_config['GENESIS_TIME']
        seconds_per_slot = self.network_config['SECONDS_PER_SLOT']
        block_timestamp = genesis_time + (slot * seconds_per_slot)
        return datetime.fromtimestamp(block_timestamp, timezone.utc).isoformat()
    
    def _get_block_timestamp(self, block: dict, slot: int) -> str:
        """Get the best available timestamp for a block"""
        timestamp_utc = block.get("timestamp_utc")
        if timestamp_utc and timestamp_utc != "1970-01-01T00:00:00+00:00":
            return timestamp_utc
        
        execution_payload = block.get("data", {}).get("message", {}).get("body", {}).get("execution_payload", {})
        if execution_payload:
            timestamp_str = execution_payload.get("timestamp", "0")
            try:
                timestamp = int(timestamp_str)
                if timestamp > 0:
                    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass
        
        return self._calculate_slot_timestamp(slot)
    
    def parse_era_range(self, era_range: str) -> Tuple[int, Optional[int]]:
        """Parse era range string into start and end values"""
        if '+' in era_range:
            start_era = int(era_range.replace('+', ''))
            return start_era, None
        elif '-' in era_range:
            start_str, end_str = era_range.split('-', 1)
            return int(start_str), int(end_str)
        else:
            era = int(era_range)
            return era, era
    
    def process_remote_eras(self, network: str, era_range: str, command: str, 
                           base_output: str, separate_files: bool = False,
                           download_only: bool = False, resume: bool = False,
                           export_type: str = "file") -> Dict[str, Any]:
        """Process remote era files"""
        print(f"🌐 Remote Era Processing")
        print(f"   Network: {network}")
        print(f"   Era range: {era_range}")
        print(f"   Command: {command}")
        print(f"   Export type: {export_type}")
        
        start_era, end_era = self.parse_era_range(era_range)
        
        try:
            downloader = get_remote_era_downloader()
            downloader.network = network
        except ValueError as e:
            print(f"❌ Configuration error: {e}")
            print("💡 Make sure to set ERA_BASE_URL environment variable")
            return {"success": False, "error": str(e)}
        
        if export_type == "clickhouse":
            try:
                from .export.clickhouse_service import ClickHouseService
                ch_service = ClickHouseService()
                processed_eras = set(ch_service.get_processed_eras(network, start_era, end_era))
                print(f"📋 Found {len(processed_eras)} already processed eras in ClickHouse")
            except Exception as e:
                print(f"⚠️  Could not check ClickHouse status: {e}")
                processed_eras = set()
        else:
            processed_eras = set()
        
        if download_only:
            print("📥 Download-only mode")
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
            return downloader.process_era_range(
                start_era=start_era,
                end_era=end_era,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume,
                export_type=export_type,
                processed_eras=processed_eras
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
    
    def extract_all_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract ALL data from blocks with full structure and proper normalization"""
        block_records = self.era_reader.get_block_records()
        
        # Initialize all possible data types including execution_payloads and sync_aggregates
        all_data = {
            'blocks': [],
            'execution_payloads': [],  # Separate execution payload table
            'sync_aggregates': [],     # NEW: separate sync aggregate table
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
        
        print("🌍 FULL DATA EXTRACTION - Getting everything with proper normalization...")
        
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
            sync_aggregate = body.get("sync_aggregate", {})
            timestamp_utc = self._get_block_timestamp(block, slot)
            
            # Blocks - ONLY beacon chain data (no execution payload or sync aggregate fields)
            all_data['blocks'].append({
                "slot": slot,
                "proposer_index": message.get("proposer_index"),
                "parent_root": message.get("parent_root"),
                "state_root": message.get("state_root"),
                "signature": block.get("data", {}).get("signature"),
                "version": block.get("version"),
                "timestamp_utc": timestamp_utc,
                "randao_reveal": body.get("randao_reveal"),
                "graffiti": body.get("graffiti"),
                "eth1_deposit_root": body.get("eth1_data", {}).get("deposit_root"),
                "eth1_deposit_count": body.get("eth1_data", {}).get("deposit_count"),
                "eth1_block_hash": body.get("eth1_data", {}).get("block_hash"),
            })
            
            # NEW: Sync Aggregates - separate normalized table (Altair+ only)
            if sync_aggregate:  # Only if sync aggregate exists
                all_data['sync_aggregates'].append({
                    "slot": slot,
                    "sync_committee_bits": sync_aggregate.get("sync_committee_bits"),
                    "sync_committee_signature": sync_aggregate.get("sync_committee_signature"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Execution Payloads - separate normalized table (Bellatrix+ only)
            if execution_payload:  # Only if execution payload exists
                all_data['execution_payloads'].append({
                    "slot": slot,
                    "parent_hash": execution_payload.get("parent_hash"),
                    "fee_recipient": execution_payload.get("fee_recipient"),
                    "state_root": execution_payload.get("state_root"),
                    "receipts_root": execution_payload.get("receipts_root"),
                    "logs_bloom": execution_payload.get("logs_bloom"),
                    "prev_randao": execution_payload.get("prev_randao"),
                    "block_number": execution_payload.get("block_number"),
                    "gas_limit": execution_payload.get("gas_limit"),
                    "gas_used": execution_payload.get("gas_used"),
                    "timestamp": execution_payload.get("timestamp"),
                    "base_fee_per_gas": execution_payload.get("base_fee_per_gas"),
                    "block_hash": execution_payload.get("block_hash"),
                    "blob_gas_used": execution_payload.get("blob_gas_used"),
                    "excess_blob_gas": execution_payload.get("excess_blob_gas"),
                    "extra_data": execution_payload.get("extra_data"),
                })
            
            # Transactions
            transactions = execution_payload.get("transactions", [])
            for tx_index, tx_hash in enumerate(transactions):
                all_data['transactions'].append({
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
                    "timestamp_utc": timestamp_utc,
                })
            
            # Withdrawals
            withdrawals = execution_payload.get("withdrawals", [])
            for withdrawal in withdrawals:
                all_data['withdrawals'].append({
                    "slot": slot,
                    "block_number": execution_payload.get("block_number"),
                    "block_hash": execution_payload.get("block_hash"),
                    "withdrawal_index": withdrawal.get("index"),
                    "validator_index": withdrawal.get("validator_index"),
                    "address": withdrawal.get("address"),
                    "amount": withdrawal.get("amount"),
                    "timestamp": execution_payload.get("timestamp"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Attestations - FULL nested data
            attestations = body.get("attestations", [])
            for att_index, attestation in enumerate(attestations):
                att_data = attestation.get("data", {})
                source = att_data.get("source", {})
                target = att_data.get("target", {})
                
                all_data['attestations'].append({
                    "slot": slot,
                    "attestation_index": att_index,
                    "aggregation_bits": attestation.get("aggregation_bits"),
                    "signature": attestation.get("signature"),
                    "attestation_slot": att_data.get("slot"),
                    "committee_index": att_data.get("index"),
                    "beacon_block_root": att_data.get("beacon_block_root"),
                    "source_epoch": source.get("epoch"),
                    "source_root": source.get("root"),
                    "target_epoch": target.get("epoch"),
                    "target_root": target.get("root"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Deposits - FULL data
            deposits = body.get("deposits", [])
            for deposit_idx, deposit in enumerate(deposits):
                deposit_data = deposit.get("data", {})
                all_data['deposits'].append({
                    "slot": slot,
                    "deposit_index": deposit_idx,
                    "proof": json.dumps(deposit.get("proof", [])),
                    "pubkey": deposit_data.get("pubkey"),
                    "withdrawal_credentials": deposit_data.get("withdrawal_credentials"),
                    "amount": deposit_data.get("amount"),
                    "signature": deposit_data.get("signature"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Voluntary Exits - FULL data
            voluntary_exits = body.get("voluntary_exits", [])
            for exit_idx, voluntary_exit in enumerate(voluntary_exits):
                exit_message = voluntary_exit.get("message", {})
                all_data['voluntary_exits'].append({
                    "slot": slot,
                    "exit_index": exit_idx,
                    "signature": voluntary_exit.get("signature"),
                    "epoch": exit_message.get("epoch"),
                    "validator_index": exit_message.get("validator_index"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Proposer Slashings - FULL data
            proposer_slashings = body.get("proposer_slashings", [])
            for slash_idx, slashing in enumerate(proposer_slashings):
                signed_header_1 = slashing.get("signed_header_1", {})
                signed_header_2 = slashing.get("signed_header_2", {})
                
                all_data['proposer_slashings'].append({
                    "slot": slot,
                    "slashing_index": slash_idx,
                    "header_1_slot": signed_header_1.get("message", {}).get("slot"),
                    "header_1_proposer_index": signed_header_1.get("message", {}).get("proposer_index"),
                    "header_1_parent_root": signed_header_1.get("message", {}).get("parent_root"),
                    "header_1_state_root": signed_header_1.get("message", {}).get("state_root"),
                    "header_1_body_root": signed_header_1.get("message", {}).get("body_root"),
                    "header_1_signature": signed_header_1.get("signature"),
                    "header_2_slot": signed_header_2.get("message", {}).get("slot"),
                    "header_2_proposer_index": signed_header_2.get("message", {}).get("proposer_index"),
                    "header_2_parent_root": signed_header_2.get("message", {}).get("parent_root"),
                    "header_2_state_root": signed_header_2.get("message", {}).get("state_root"),
                    "header_2_body_root": signed_header_2.get("message", {}).get("body_root"),
                    "header_2_signature": signed_header_2.get("signature"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Attester Slashings - FULL data
            attester_slashings = body.get("attester_slashings", [])
            for slash_idx, slashing in enumerate(attester_slashings):
                attestation_1 = slashing.get("attestation_1", {})
                attestation_2 = slashing.get("attestation_2", {})
                
                all_data['attester_slashings'].append({
                    "slot": slot,
                    "slashing_index": slash_idx,
                    "att_1_slot": attestation_1.get("data", {}).get("slot"),
                    "att_1_committee_index": attestation_1.get("data", {}).get("index"),
                    "att_1_beacon_block_root": attestation_1.get("data", {}).get("beacon_block_root"),
                    "att_1_source_epoch": attestation_1.get("data", {}).get("source", {}).get("epoch"),
                    "att_1_target_epoch": attestation_1.get("data", {}).get("target", {}).get("epoch"),
                    "att_1_signature": attestation_1.get("signature"),
                    "att_2_slot": attestation_2.get("data", {}).get("slot"),
                    "att_2_committee_index": attestation_2.get("data", {}).get("index"),
                    "att_2_beacon_block_root": attestation_2.get("data", {}).get("beacon_block_root"),
                    "att_2_source_epoch": attestation_2.get("data", {}).get("source", {}).get("epoch"),
                    "att_2_target_epoch": attestation_2.get("data", {}).get("target", {}).get("epoch"),
                    "att_2_signature": attestation_2.get("signature"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # BLS Changes - FULL data (Capella+)
            bls_changes = body.get("bls_to_execution_changes", [])
            for change_idx, bls_change in enumerate(bls_changes):
                change_message = bls_change.get("message", {})
                all_data['bls_changes'].append({
                    "slot": slot,
                    "change_index": change_idx,
                    "signature": bls_change.get("signature"),
                    "validator_index": change_message.get("validator_index"),
                    "from_bls_pubkey": change_message.get("from_bls_pubkey"),
                    "to_execution_address": change_message.get("to_execution_address"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Blob Commitments - FULL data (Deneb+)
            blob_commitments = body.get("blob_kzg_commitments", [])
            for commit_idx, commitment in enumerate(blob_commitments):
                all_data['blob_commitments'].append({
                    "slot": slot,
                    "commitment_index": commit_idx,
                    "commitment": commitment,
                    "timestamp_utc": timestamp_utc,
                })
            
            # Execution Requests - FULL data (Electra+)
            execution_requests = body.get("execution_requests", {})
            
            # Deposit requests
            deposit_requests = execution_requests.get("deposits", [])
            for req_idx, deposit_req in enumerate(deposit_requests):
                all_data['execution_requests'].append({
                    "slot": slot,
                    "request_type": "deposit",
                    "request_index": req_idx,
                    "pubkey": deposit_req.get("pubkey"),
                    "withdrawal_credentials": deposit_req.get("withdrawal_credentials"),
                    "amount": deposit_req.get("amount"),
                    "signature": deposit_req.get("signature"),
                    "deposit_request_index": deposit_req.get("index"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Withdrawal requests
            withdrawal_requests = execution_requests.get("withdrawals", [])
            for req_idx, withdrawal_req in enumerate(withdrawal_requests):
                all_data['execution_requests'].append({
                    "slot": slot,
                    "request_type": "withdrawal",
                    "request_index": req_idx,
                    "source_address": withdrawal_req.get("source_address"),
                    "validator_pubkey": withdrawal_req.get("validator_pubkey"),
                    "amount": withdrawal_req.get("amount"),
                    "timestamp_utc": timestamp_utc,
                })
            
            # Consolidation requests  
            consolidation_requests = execution_requests.get("consolidations", [])
            for req_idx, consolidation_req in enumerate(consolidation_requests):
                all_data['execution_requests'].append({
                    "slot": slot,
                    "request_type": "consolidation", 
                    "request_index": req_idx,
                    "source_address": consolidation_req.get("source_address"),
                    "source_pubkey": consolidation_req.get("source_pubkey"),
                    "target_pubkey": consolidation_req.get("target_pubkey"),
                    "timestamp_utc": timestamp_utc,
                })
        
        print(f"✅ Successfully processed {successful} blocks with FULL normalized data extraction")
        return all_data
    
    def extract_specific_data(self, data_type: str) -> List[Dict[str, Any]]:
        """Extract specific data type from all blocks"""
        all_data = self.extract_all_data()
        return all_data.get(data_type, [])
    
    def export_data(self, data, output_file: str, data_type: str = "blocks", separate_files: bool = False, export_type: str = "file"):
        """Export data using appropriate exporter"""
        era_info = self.era_reader.get_era_info()
        
        if export_type == "clickhouse":
            # ClickHouse ALWAYS acts like --separate flag is on
            exporter = ClickHouseExporter(era_info, self.era_reader.filepath)
            if isinstance(data, dict):
                # Multiple data types - load all at once
                print(f"📊 Loading all data types to ClickHouse:")
                for table_name, table_data in data.items():
                    if table_data:
                        print(f"   - {table_name}: {len(table_data)} records")
                    else:
                        print(f"   - {table_name}: 0 records (empty)")
                exporter.load_all_data_types(data)
            else:
                # Single data type
                print(f"📊 Loading {len(data)} records into {data_type} table")
                exporter.load_data_to_table(data, data_type)
            return
        
        # File export
        if output_file.endswith(('.json', '.jsonl')):
            exporter = JSONExporter(era_info)
            if isinstance(data, dict) and separate_files:
                for data_name, data_list in data.items():
                    if data_list:
                        file_name = output_file.replace('.json', f'_{data_name}.json').replace('.jsonl', f'_{data_name}.jsonl')
                        print(f"📝 Exporting {len(data_list)} {data_name} records to {file_name}")
                        exporter.export_data_type(data_list, file_name, data_name)
                    else:
                        print(f"📝 Skipping {data_name} (no records)")
            else:
                if data_type == "blocks":
                    exporter.export_blocks(data, output_file)
                else:
                    exporter.export_data_type(data, output_file, data_type)
                
        elif output_file.endswith('.csv'):
            exporter = CSVExporter(era_info)
            if isinstance(data, dict) and separate_files:
                for data_name, data_list in data.items():
                    if data_list:
                        file_name = output_file.replace('.csv', f'_{data_name}.csv')
                        print(f"📝 Exporting {len(data_list)} {data_name} records to {file_name}")
                        exporter.export_data_type(data_list, file_name, data_name)
                    else:
                        print(f"📝 Skipping {data_name} (no records)")
            else:
                if data_type == "blocks":
                    exporter.export_blocks(data, output_file)
                else:
                    exporter.export_data_type(data, output_file, data_type)
                
        elif output_file.endswith('.parquet'):
            exporter = ParquetExporter(era_info)
            if isinstance(data, dict) and separate_files:
                for data_name, data_list in data.items():
                    if data_list:
                        file_name = output_file.replace('.parquet', f'_{data_name}.parquet')
                        print(f"📝 Exporting {len(data_list)} {data_name} records to {file_name}")
                        exporter.export_data_type(data_list, file_name, data_name)
                    else:
                        print(f"📝 Skipping {data_name} (no records)")
            else:
                if data_type == "blocks":
                    exporter.export_blocks(data, output_file)
                else:
                    exporter.export_data_type(data, output_file, data_type)
        
        else:
            raise ValueError(f"Unsupported output format: {output_file}")
    
    def _process_single_era(self, command: str, output_file: str, separate_files: bool, export_type: str = "file") -> bool:
        """Process a single era file"""
        try:
            print(f"   🔧 Processing with command: '{command}'")
            if export_type == "file":
                print(f"   📁 Output file: '{output_file}'")
            else:
                print(f"   🗄️  Export to: {export_type}")
            print(f"   🔀 Separate files: {separate_files}")
            
            if command == "all-blocks":
                if separate_files or export_type == "clickhouse":
                    all_data = self.extract_all_data()
                    self.export_data(all_data, output_file, "all", separate_files=True, export_type=export_type)
                else:
                    blocks = self.parse_all_blocks()
                    self.export_data(blocks, output_file, "blocks", export_type=export_type)
                    
            elif command in ["transactions", "withdrawals", "attestations", "sync_aggregates"]:
                data = self.extract_specific_data(command)
                self.export_data(data, output_file, command, export_type=export_type)
            else:
                print(f"❌ Unknown command for processing: '{command}'")
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ Error during processing: {e}")
            import traceback
            traceback.print_exc()
            return False

    def show_era_processing_status(self, network: str = None):
        """Show era processing status with granular dataset tracking"""
        try:
            state_manager = EraStateManager()
            summary = state_manager.get_processing_summary(network)
            
            print(f"📊 Era Processing Status" + (f" ({network})" if network else " (All Networks)"))
            print("="*60)
            
            # Era-level summary
            if summary['era_summary']:
                print("\n📁 Era Summary:")
                for net, stats in summary['era_summary'].items():
                    print(f"  {net}:")
                    print(f"    Total Eras: {stats['total_eras']}")
                    print(f"    Fully Completed: {stats['fully_completed_eras']}")
                    print(f"    Processing: {stats['processing_eras']}")
                    print(f"    Failed: {stats['fully_failed_eras']}")
                    print(f"    Total Rows: {stats['total_rows']:,}")
                    
                    if stats['total_eras'] > 0:
                        completion_pct = (stats['fully_completed_eras'] / stats['total_eras']) * 100
                        print(f"    Completion: {completion_pct:.1f}%")
            
            # Dataset-level summary
            if summary['dataset_summary']:
                print("\n📊 Dataset Summary:")
                for net, datasets in summary['dataset_summary'].items():
                    print(f"  {net}:")
                    for dataset, stats in datasets.items():
                        print(f"    {dataset}:")
                        print(f"      Completed Eras: {stats['completed_eras']}")
                        print(f"      Failed Eras: {stats['failed_eras']}")
                        print(f"      Total Rows: {stats['total_rows']:,}")
                        print(f"      Highest Era: {stats['highest_completed_era']}")
            
            if not summary['era_summary'] and not summary['dataset_summary']:
                print("No processing data found.")
                
        except Exception as e:
            print(f"❌ Error getting status: {e}")

    def show_era_failed_datasets(self, network: str = None, limit: int = 20):
        """Show failed datasets for retry"""
        try:
            state_manager = EraStateManager()
            failed = state_manager.get_failed_datasets(network, limit)
            
            print(f"❌ Failed Datasets" + (f" ({network})" if network else " (All Networks)"))
            print("="*60)
            
            if not failed:
                print("No failed datasets found.")
                return
            
            for failure in failed:
                print(f"Era: {failure['era_filename']}")
                print(f"  Dataset: {failure['dataset']}")
                print(f"  Network: {failure['network']}")
                print(f"  Era Number: {failure['era_number']}")
                print(f"  Attempts: {failure['attempt_count']}")
                print(f"  Error: {failure['error_message'][:100]}...")
                print(f"  Failed At: {failure['created_at']}")
                print()
                
        except Exception as e:
            print(f"❌ Error getting failed datasets: {e}")

    def cleanup_era_stale_processing(self, timeout_minutes: int = 30):
        """Cleanup stale era processing entries"""
        try:
            state_manager = EraStateManager()
            reset_count = state_manager.cleanup_stale_processing(timeout_minutes)
            
            if reset_count > 0:
                print(f"✅ Reset {reset_count} stale processing entries")
            else:
                print("No stale processing entries found")
                
        except Exception as e:
            print(f"❌ Error cleaning up stale processing: {e}")

    def check_era_status(self, era_file: str):
        """Check processing status of a specific era file"""
        try:
            state_manager = EraStateManager()
            era_filename = state_manager.get_era_filename_from_path(era_file)
            
            # Check if fully processed
            is_complete = state_manager.is_era_fully_processed(era_filename)
            pending_datasets = state_manager.get_pending_datasets(era_filename)
            
            print(f"📋 Era Status: {era_filename}")
            print("="*60)
            print(f"Fully Processed: {'✅ Yes' if is_complete else '❌ No'}")
            
            if pending_datasets:
                print(f"Pending Datasets ({len(pending_datasets)}):")
                for dataset in pending_datasets:
                    print(f"  - {dataset}")
            else:
                print("All datasets completed ✅")
                
        except Exception as e:
            print(f"❌ Error checking era status: {e}")


def main():
    """Simple main CLI entry point"""
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
        print("  era-parser <era_file> sync_aggregates <output_file>   # Sync aggregate data only")
        print("  era-parser <era_file> stats                           # Show era statistics")
        print("")
        print("CLICKHOUSE EXPORT:")
        print("  era-parser <era_file> all-blocks --export clickhouse  # Export to ClickHouse")
        print("  era-parser <era_file> transactions --export clickhouse # Export transactions to ClickHouse")
        print("  era-parser <era_file> sync_aggregates --export clickhouse # Export sync aggregates to ClickHouse")
        print("")
        print("REMOTE ERA PROCESSING:")
        print("  era-parser --remote <network> <era_range> <command> <o>           # Process remote eras")
        print("  era-parser --remote <network> <era_range> <command> <o> --separate  # Separate files")
        print("  era-parser --remote <network> <era_range> <command> --export clickhouse # Remote to ClickHouse")
        print("  era-parser --remote <network> <era_range> --download-only              # Download only")
        print("  era-parser --remote <network> <era_range> <command> <o> --resume # Resume processing")
        print("")
        print("ERA STATE MANAGEMENT:")
        print("  era-parser --era-status <network|all>                 # Show era processing status")
        print("  era-parser --era-failed <network|all> [limit]         # Show failed datasets")
        print("  era-parser --era-cleanup [timeout_minutes]            # Clean stale processing")
        print("  era-parser --era-check <era_file>                     # Check specific era status")
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
        print("NOTES:")
        print("  - Era state management provides granular dataset tracking")
        print("  - ClickHouse export ALWAYS creates separate tables (like --separate)")
        print("  - Parquet with --separate creates one file per data type")
        print("  - All nested data is fully extracted and preserved")
        print("  - sync_aggregates are now extracted as separate table/files")
        sys.exit(1)
    
    cli = EraParserCLI()
    
    # Handle batch processing commands
    if sys.argv[1] == "--batch":
        if len(sys.argv) < 5:
            print("Usage: era-parser --batch <pattern> <command> <base_output> [--separate] [--export clickhouse]")
            sys.exit(1)
        
        pattern = sys.argv[2]
        command = sys.argv[3]
        base_output = sys.argv[4]
        separate_files = "--separate" in sys.argv
        export_type = "clickhouse" if "--export" in sys.argv and "clickhouse" in sys.argv else "file"
        
        try:
            era_files = cli.find_era_files(pattern)
            
            if not era_files:
                print(f"❌ No era files found matching pattern: {pattern}")
                sys.exit(1)
            
            print(f"🔍 Found {len(era_files)} era files to process")
            
            processed_count = 0
            failed_count = 0
            
            for i, era_file in enumerate(era_files, 1):
                print(f"\n{'='*60}")
                print(f"📈 Processing era file {i}/{len(era_files)}: {os.path.basename(era_file)}")
                print(f"{'='*60}")
                
                try:
                    cli.setup(era_file)
                    
                    if export_type == "file":
                        era_info = cli.era_reader.get_era_info()
                        era_number = era_info.get('era_number', i)
                        if base_output.endswith(('.json', '.csv', '.parquet')):
                            base_name = base_output.rsplit('.', 1)[0]
                            extension = '.' + base_output.rsplit('.', 1)[1]
                            output_file = f"{base_name}_era_{era_number:05d}{extension}"
                        else:
                            output_file = f"{base_output}_era_{era_number:05d}.parquet"
                        
                        print(f"   📂 Output: {output_file}")
                    else:
                        output_file = "clickhouse_output"
                        print(f"   🗄️  Output: ClickHouse")
                    
                    success = cli._process_single_era(command, output_file, separate_files, export_type)
                    
                    if success:
                        processed_count += 1
                        print(f"✅ Successfully processed {os.path.basename(era_file)}")
                    else:
                        failed_count += 1
                        print(f"❌ Failed to process {os.path.basename(era_file)}")
                        
                except Exception as e:
                    failed_count += 1
                    print(f"❌ Error processing {os.path.basename(era_file)}: {e}")
            
            print(f"\n{'='*60}")
            print(f"🎉 BATCH PROCESSING COMPLETE!")
            print(f"{'='*60}")
            print(f"✅ Successfully processed: {processed_count}/{len(era_files)} files")
            print(f"❌ Failed: {failed_count} files")
            
        except Exception as e:
            print(f"❌ Batch processing error: {e}")
            sys.exit(1)
    
    # Handle remote processing commands
    elif sys.argv[1] == "--remote":
        if len(sys.argv) < 5:
            print("Usage: era-parser --remote <network> <era_range> <command> [<output>] [--separate] [--resume] [--export clickhouse]")
            print("   or: era-parser --remote <network> <era_range> --download-only")
            sys.exit(1)
        
        network = sys.argv[2]
        era_range = sys.argv[3]
        
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
        
        if len(sys.argv) < 5:
            print("Usage: era-parser --remote <network> <era_range> <command> [<output>] [--separate] [--resume] [--export clickhouse]")
            sys.exit(1)
        
        command = sys.argv[4]
        base_output = sys.argv[5] if len(sys.argv) > 5 and not sys.argv[5].startswith('--') else "output"
        separate_files = "--separate" in sys.argv
        resume = "--resume" in sys.argv
        export_type = "clickhouse" if "--export" in sys.argv and "clickhouse" in sys.argv else "file"
        
        try:
            result = cli.process_remote_eras(
                network=network,
                era_range=era_range,
                command=command,
                base_output=base_output,
                separate_files=separate_files,
                resume=resume,
                export_type=export_type
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
    
    # Handle era processing status commands
    elif sys.argv[1] == "--era-status":
        if len(sys.argv) < 3:
            print("Usage: era-parser --era-status [network]")
            print("       era-parser --era-status all")
            sys.exit(1)
        
        network = sys.argv[2] if sys.argv[2] != 'all' else None
        cli.show_era_processing_status(network)
    
    elif sys.argv[1] == "--era-failed":
        if len(sys.argv) < 3:
            print("Usage: era-parser --era-failed [network] [limit]")
            print("       era-parser --era-failed all")
            sys.exit(1)
        
        network = sys.argv[2] if sys.argv[2] != 'all' else None
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else 20
        cli.show_era_failed_datasets(network, limit)
    
    elif sys.argv[1] == "--era-cleanup":
        timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        cli.cleanup_era_stale_processing(timeout)
    
    elif sys.argv[1] == "--era-check":
        if len(sys.argv) < 3:
            print("Usage: era-parser --era-check <era_file>")
            sys.exit(1)
        
        era_file = sys.argv[2]
        cli.check_era_status(era_file)
     
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
    
    # Handle local file processing
    else:
        era_file = sys.argv[1]
        command = sys.argv[2] if len(sys.argv) > 2 else "help"
        
        if not os.path.exists(era_file):
            print(f"❌ Era file not found: {era_file}")
            sys.exit(1)
        
        # Check for flags
        separate_files = "--separate" in sys.argv
        export_type = "clickhouse" if "--export" in sys.argv and "clickhouse" in sys.argv else "file"
        
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
                if len(sys.argv) < 4 and export_type == "file":
                    print("Usage: era-parser <era_file> all-blocks <output_file> [--separate] [--export clickhouse]")
                    sys.exit(1)
                output_file = sys.argv[3] if export_type == "file" else "clickhouse_output"
                
                if separate_files or export_type == "clickhouse":
                    all_data = cli.extract_all_data()
                    cli.export_data(all_data, output_file, "all", separate_files=True, export_type=export_type)
                else:
                    blocks = cli.parse_all_blocks()
                    cli.export_data(blocks, output_file, "blocks", export_type=export_type)
                
            elif command in ["transactions", "withdrawals", "attestations", "sync_aggregates"]:
                if len(sys.argv) < 4 and export_type == "file":
                    print(f"Usage: era-parser <era_file> {command} <output_file> [--export clickhouse]")
                    sys.exit(1)
                output_file = sys.argv[3] if export_type == "file" else "clickhouse_output"
                data = cli.extract_specific_data(command)
                cli.export_data(data, output_file, command, export_type=export_type)
                
            else:
                print(f"❌ Unknown command: {command}")
                print("Available commands: block, all-blocks, transactions, withdrawals, attestations, sync_aggregates, stats")
                sys.exit(1)
                
            print(f"🎉 Operation completed successfully!")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    main()