"""Core era processing logic extracted from original CLI"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from ..ingestion import EraReader
from ..parsing import BlockParser
from ..export import JSONExporter, CSVExporter, ParquetExporter, ClickHouseExporter
from ..config import detect_network_from_filename, get_network_config

class EraProcessor:
    """Core era processing functionality"""
    
    def __init__(self):
        self.network = None
        self.network_config = None
        self.era_reader = None
        self.block_parser = None
    
    def setup(self, era_file: str, network: str = None):
        """Setup processor with era file"""
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
    
    def show_stats(self):
        """Show era file statistics"""
        era_info = self.era_reader.get_era_info()
        stats = self.era_reader.get_statistics()
        
        print(f"📊 Era File Statistics: {self.era_reader.filepath.split('/')[-1]}")
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
        """Extract ALL data from blocks with SIMPLIFIED single timestamp approach"""
        block_records = self.era_reader.get_block_records()
        
        # Initialize all possible data types
        all_data = {
            'blocks': [],
            'execution_payloads': [],  
            'sync_aggregates': [],     
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
        
        print("🌍 FULL DATA EXTRACTION - SIMPLIFIED single timestamp approach...")
        
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
            
            # Blocks - ONLY beacon chain data with SINGLE timestamp
            all_data['blocks'].append({
                "slot": slot,
                "proposer_index": message.get("proposer_index"),
                "parent_root": message.get("parent_root"),
                "state_root": message.get("state_root"),
                "signature": block.get("data", {}).get("signature"),
                "version": block.get("version"),
                "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                "randao_reveal": body.get("randao_reveal"),
                "graffiti": body.get("graffiti"),
                "eth1_deposit_root": body.get("eth1_data", {}).get("deposit_root"),
                "eth1_deposit_count": body.get("eth1_data", {}).get("deposit_count"),
                "eth1_block_hash": body.get("eth1_data", {}).get("block_hash"),
            })
            
            # Sync Aggregates - SINGLE timestamp
            if sync_aggregate:
                all_data['sync_aggregates'].append({
                    "slot": slot,
                    "sync_committee_bits": sync_aggregate.get("sync_committee_bits"),
                    "sync_committee_signature": sync_aggregate.get("sync_committee_signature"),
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Execution Payloads - SINGLE timestamp (NO duplicate timestamp field)
            if execution_payload:
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp - NO duplicate!
                    "base_fee_per_gas": execution_payload.get("base_fee_per_gas"),
                    "block_hash": execution_payload.get("block_hash"),
                    "blob_gas_used": execution_payload.get("blob_gas_used"),
                    "excess_blob_gas": execution_payload.get("excess_blob_gas"),
                    "extra_data": execution_payload.get("extra_data"),
                })
            
            # Transactions - SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp - NO duplicate
                })
            
            # Withdrawals - SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp - NO duplicate
                })
            
            # Attestations - FULL nested data with SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Deposits - Handle properly parsed deposit structure with SINGLE timestamp
            deposits = body.get("deposits", [])
            for deposit_idx, deposit in enumerate(deposits):
                deposit_data = deposit.get("data", {})
                proof = deposit.get("proof", [])
                
                # Create deposit record with unnested data fields
                deposit_record = {
                    "slot": slot,
                    "deposit_index": deposit_idx,
                    # Unnested data fields (moved from data.* to top level)
                    "pubkey": deposit_data.get("pubkey", ""),
                    "withdrawal_credentials": deposit_data.get("withdrawal_credentials", ""),
                    "amount": deposit_data.get("amount", "0"),
                    "signature": deposit_data.get("signature", ""),
                    # Proof as JSON array (much cleaner than 33 individual columns)
                    "proof": json.dumps(proof) if proof else "[]",
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                }
                
                all_data['deposits'].append(deposit_record)
            
            # Voluntary Exits - FULL data with SINGLE timestamp
            voluntary_exits = body.get("voluntary_exits", [])
            for exit_idx, voluntary_exit in enumerate(voluntary_exits):
                exit_message = voluntary_exit.get("message", {})
                all_data['voluntary_exits'].append({
                    "slot": slot,
                    "exit_index": exit_idx,
                    "signature": voluntary_exit.get("signature"),
                    "epoch": exit_message.get("epoch"),
                    "validator_index": exit_message.get("validator_index"),
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Proposer Slashings - FULL data with SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Attester Slashings - FULL data with SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # BLS Changes - FULL data with SINGLE timestamp (Capella+)
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Blob Commitments - FULL data with SINGLE timestamp (Deneb+)
            blob_commitments = body.get("blob_kzg_commitments", [])
            for commit_idx, commitment in enumerate(blob_commitments):
                all_data['blob_commitments'].append({
                    "slot": slot,
                    "commitment_index": commit_idx,
                    "commitment": commitment,
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
            
            # Execution Requests - FULL data with SINGLE timestamp (Electra+)
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
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
                    "timestamp_utc": timestamp_utc,  # SINGLE timestamp
                })
        
        print(f"✅ Successfully processed {successful} blocks with SIMPLIFIED single timestamp extraction")
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
    
    def process_single_era(self, command: str, output_file: str, separate_files: bool, export_type: str = "file") -> bool:
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