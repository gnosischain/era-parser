import json
import struct
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple

from ..ssz_utils import read_uint32_at, read_uint64_at, parse_list_of_items

class BaseForkParser(ABC):
    """Base class for all fork parsers with self-describing item parsers"""
    
    # Define block body schema structure - to be overridden by subclasses
    BODY_SCHEMA = []
    
    def parse_fixed_fields(self, body_data: bytes) -> tuple:
        """Parse fixed fields common to all forks (first 200 bytes)"""
        pos = 0
        
        # randao_reveal (96 bytes)
        randao_reveal = "0x" + body_data[pos:pos+96].hex()
        pos += 96
        
        # eth1_data (72 bytes)
        eth1_data_raw = body_data[pos:pos+72]
        eth1_data = {
            "deposit_root": "0x" + eth1_data_raw[0:32].hex(),
            "deposit_count": str(read_uint64_at(eth1_data_raw, 32)),
            "block_hash": "0x" + eth1_data_raw[40:72].hex()
        }
        pos += 72
        
        # graffiti (32 bytes)
        graffiti = "0x" + body_data[pos:pos+32].hex()
        pos += 32
        
        return randao_reveal, eth1_data, graffiti, pos
    
    def parse_base_variable_fields(self, body_data: bytes, start_pos: int) -> tuple:
        """Parse the 5 base variable fields present in all forks"""
        base_offsets = []
        for i in range(5):
            offset = read_uint32_at(body_data, start_pos + i * 4)
            base_offsets.append(offset)
        
        return base_offsets, start_pos + 20

    def _parse_body_from_schema(self, body_data: bytes) -> Dict[str, Any]:
        """Generic body parser using BODY_SCHEMA definition"""
        result = {}
        
        # Parse fixed fields (200 bytes)
        randao_reveal, eth1_data, graffiti, pos = self.parse_fixed_fields(body_data)
        result.update({
            "randao_reveal": randao_reveal,
            "eth1_data": eth1_data,
            "graffiti": graffiti
        })
        
        # Parse base variable fields (5 fields)
        base_offsets, pos = self.parse_base_variable_fields(body_data, pos)
        
        # Process additional fixed fields (like sync_aggregate)
        fixed_fields = [field for field in self.BODY_SCHEMA if field[0] == 'fixed']
        for field_type, field_name, size_or_parser in fixed_fields:
            if pos + size_or_parser <= len(body_data):
                if field_name == 'sync_aggregate':
                    result[field_name] = self.parse_sync_aggregate(body_data[pos:pos+size_or_parser])
                pos += size_or_parser
            else:
                result[field_name] = {}
        
        # Parse additional variable field offsets
        variable_fields = [field for field in self.BODY_SCHEMA if field[0] == 'variable']
        additional_offsets = []
        for field_type, field_name, parser_info in variable_fields:
            if pos + 4 <= len(body_data):
                offset = read_uint32_at(body_data, pos)
                additional_offsets.append(offset)
                pos += 4
        
        # Combine all offsets and field definitions
        all_offsets = base_offsets + additional_offsets
        all_field_definitions = self.get_base_field_definitions()
        
        for field_type, field_name, parser_info in variable_fields:
            if isinstance(parser_info, str):
                # Method name string
                parser_method = getattr(self, parser_info)
                all_field_definitions.append((field_name, parser_method))
            elif isinstance(parser_info, tuple):
                # (function, method_name) tuple
                func, method_name = parser_info
                parser_method = getattr(self, method_name)
                all_field_definitions.append((field_name, func, parser_method))
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, all_offsets, all_field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present
        expected_fields = [field[1] for field in self.BODY_SCHEMA] + [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result

    # Self-describing item parsers with ssz_size attribute
    def parse_deposit(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single deposit"""
        try:
            if len(data) < 1240:
                return None
            
            pos = 0
            
            # Parse proof: Vector[Bytes32, 33]
            proof = []
            for i in range(33):
                if pos + 32 > len(data):
                    return None
                hash_bytes = data[pos:pos+32]
                proof.append("0x" + hash_bytes.hex())
                pos += 32
            
            # Parse deposit data
            if pos + 184 > len(data):
                return None
            
            deposit_data_raw = data[pos:pos+184]
            data_pos = 0
            
            pubkey = "0x" + deposit_data_raw[data_pos:data_pos+48].hex()
            data_pos += 48
            withdrawal_credentials = "0x" + deposit_data_raw[data_pos:data_pos+32].hex()
            data_pos += 32
            amount = str(read_uint64_at(deposit_data_raw, data_pos))
            data_pos += 8
            signature = "0x" + deposit_data_raw[data_pos:data_pos+96].hex()
            
            return {
                "proof": proof,
                "data": {
                    "pubkey": pubkey,
                    "withdrawal_credentials": withdrawal_credentials,
                    "amount": amount,
                    "signature": signature
                }
            }
            
        except Exception as e:
            return None
    
    # Set SSZ size for fixed-size items
    parse_deposit.ssz_size = 1240

    def parse_attestation(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single attestation"""
        try:
            if len(data) < 228:
                return None
                
            bits_offset = read_uint32_at(data, 0)
            att_data_raw = data[4:132]
            signature = data[132:228]
            
            attestation_data = {
                "slot": str(read_uint64_at(att_data_raw, 0)), 
                "index": str(read_uint64_at(att_data_raw, 8)),
                "beacon_block_root": "0x" + att_data_raw[16:48].hex(),
                "source": {
                    "epoch": str(read_uint64_at(att_data_raw, 48)), 
                    "root": "0x" + att_data_raw[56:88].hex()
                },
                "target": {
                    "epoch": str(read_uint64_at(att_data_raw, 88)), 
                    "root": "0x" + att_data_raw[96:128].hex()
                }
            }
            
            return {
                "aggregation_bits": "0x" + data[bits_offset:].hex(), 
                "data": attestation_data, 
                "signature": "0x" + signature.hex()
            }
        except Exception:
            return None
    
    # Variable size - no ssz_size attribute
    
    def parse_voluntary_exit(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single voluntary exit"""
        try:
            if len(data) < 112:
                return None
            
            pos = 0
            epoch = str(read_uint64_at(data, pos))
            pos += 8
            validator_index = str(read_uint64_at(data, pos))
            pos += 8
            
            if pos + 96 > len(data):
                return None
                
            signature = "0x" + data[pos:pos + 96].hex()
            
            return {
                "message": {
                    "epoch": epoch,
                    "validator_index": validator_index
                },
                "signature": signature
            }
            
        except Exception as e:
            return None
    
    parse_voluntary_exit.ssz_size = 112

    def parse_proposer_slashing(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single proposer slashing"""
        try:
            if len(data) < 416:
                return None
            
            pos = 0
            
            # Parse signed_header_1
            header_1_message = {
                "slot": str(read_uint64_at(data, pos)),
                "proposer_index": str(read_uint64_at(data, pos + 8)),
                "parent_root": "0x" + data[pos + 16:pos + 48].hex(),
                "state_root": "0x" + data[pos + 48:pos + 80].hex(),
                "body_root": "0x" + data[pos + 80:pos + 112].hex()
            }
            pos += 112
            
            header_1_signature = "0x" + data[pos:pos + 96].hex()
            pos += 96
            
            signed_header_1 = {
                "message": header_1_message,
                "signature": header_1_signature
            }
            
            # Parse signed_header_2
            header_2_message = {
                "slot": str(read_uint64_at(data, pos)),
                "proposer_index": str(read_uint64_at(data, pos + 8)),
                "parent_root": "0x" + data[pos + 16:pos + 48].hex(),
                "state_root": "0x" + data[pos + 48:pos + 80].hex(),
                "body_root": "0x" + data[pos + 80:pos + 112].hex()
            }
            pos += 112
            
            header_2_signature = "0x" + data[pos:pos + 96].hex()
            
            signed_header_2 = {
                "message": header_2_message,
                "signature": header_2_signature
            }
            
            return {
                "signed_header_1": signed_header_1,
                "signed_header_2": signed_header_2
            }
            
        except Exception as e:
            return None
    
    parse_proposer_slashing.ssz_size = 416

    def parse_indexed_attestation(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse an IndexedAttestation"""
        try:
            if len(data) < 232:
                return None
            
            indices_offset = read_uint32_at(data, 0)
            
            if indices_offset >= len(data):
                return None
            
            att_data_start = 4
            att_data_raw = data[att_data_start:att_data_start + 128]
            
            attestation_data = {
                "slot": str(read_uint64_at(att_data_raw, 0)), 
                "index": str(read_uint64_at(att_data_raw, 8)),
                "beacon_block_root": "0x" + att_data_raw[16:48].hex(),
                "source": {
                    "epoch": str(read_uint64_at(att_data_raw, 48)), 
                    "root": "0x" + att_data_raw[56:88].hex()
                },
                "target": {
                    "epoch": str(read_uint64_at(att_data_raw, 88)), 
                    "root": "0x" + att_data_raw[96:128].hex()
                }
            }
            
            signature_start = att_data_start + 128
            signature = "0x" + data[signature_start:signature_start + 96].hex()
            
            indices_data = data[indices_offset:]
            attesting_indices = []
            
            if len(indices_data) >= 8:
                for i in range(0, len(indices_data) - 7, 8):
                    index = read_uint64_at(indices_data, i)
                    attesting_indices.append(str(index))
            
            return {
                "attesting_indices": attesting_indices,
                "data": attestation_data,
                "signature": signature
            }
            
        except Exception as e:
            return None

    def parse_attester_slashing(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single attester slashing"""
        try:
            if len(data) < 8:
                return None
            
            attestation_1_offset = read_uint32_at(data, 0)
            attestation_2_offset = read_uint32_at(data, 4)
            
            if attestation_1_offset >= len(data) or attestation_2_offset >= len(data):
                return None
            
            attestation_1_end = attestation_2_offset
            attestation_1_data = data[attestation_1_offset:attestation_1_end]
            attestation_1 = self.parse_indexed_attestation(attestation_1_data)
            
            attestation_2_data = data[attestation_2_offset:]
            attestation_2 = self.parse_indexed_attestation(attestation_2_data)
            
            if not attestation_1 or not attestation_2:
                return None
            
            return {
                "attestation_1": attestation_1,
                "attestation_2": attestation_2
            }
            
        except Exception as e:
            return None

    def parse_bls_to_execution_change(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single BLS to execution change"""
        try:
            if len(data) < 172:
                return None
            
            pos = 0
            validator_index = str(read_uint64_at(data, pos))
            pos += 8
            from_bls_pubkey = "0x" + data[pos:pos + 48].hex()
            pos += 48
            to_execution_address = "0x" + data[pos:pos + 20].hex()
            pos += 20
            
            if pos + 96 > len(data):
                return None
                
            signature = "0x" + data[pos:pos + 96].hex()
            
            return {
                "message": {
                    "validator_index": validator_index,
                    "from_bls_pubkey": from_bls_pubkey,
                    "to_execution_address": to_execution_address
                },
                "signature": signature
            }
            
        except Exception as e:
            return None
    
    parse_bls_to_execution_change.ssz_size = 172
    
    def parse_sync_aggregate(self, data: bytes) -> Dict[str, Any]:
        """Parse sync_aggregate"""
        if len(data) < 160: 
            return {}
        return {
            "sync_committee_bits": "0x" + data[0:64].hex(), 
            "sync_committee_signature": "0x" + data[64:160].hex()
        }
    
    def parse_withdrawal(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse execution payload withdrawal"""
        if len(data) < 44:
            return None
        return {
            "index": str(read_uint64_at(data, 0)), 
            "validator_index": str(read_uint64_at(data, 8)),
            "address": "0x" + data[16:36].hex(), 
            "amount": str(read_uint64_at(data, 36))
        }
    
    parse_withdrawal.ssz_size = 44
    
    def parse_kzg_commitment(self, data: bytes) -> Optional[str]:
        """Parse KZG commitment"""
        if len(data) != 48:
            return None
        return "0x" + data.hex()
    
    parse_kzg_commitment.ssz_size = 48

    # Execution payload parsing methods (unchanged)
    def parse_execution_payload_base(self, data: bytes, fork: str = "bellatrix") -> Tuple[Dict[str, Any], int, Dict[str, int]]:
        """Parse the common part of execution payload"""
        if len(data) < 100:
            return {}, 0, {}
            
        result, pos = {}, 0
        result["parent_hash"] = "0x" + data[pos:pos+32].hex(); pos += 32
        result["fee_recipient"] = "0x" + data[pos:pos+20].hex(); pos += 20
        result["state_root"] = "0x" + data[pos:pos+32].hex(); pos += 32
        result["receipts_root"] = "0x" + data[pos:pos+32].hex(); pos += 32
        result["logs_bloom"] = "0x" + data[pos:pos+256].hex(); pos += 256
        result["prev_randao"] = "0x" + data[pos:pos+32].hex(); pos += 32
        result["block_number"] = str(read_uint64_at(data, pos)); pos += 8
        result["gas_limit"] = str(read_uint64_at(data, pos)); pos += 8
        result["gas_used"] = str(read_uint64_at(data, pos)); pos += 8
        result["timestamp"] = str(read_uint64_at(data, pos)); pos += 8
        
        offsets = {}
        offsets["extra_data"] = read_uint32_at(data, pos); pos += 4
        result["base_fee_per_gas"] = str(int.from_bytes(data[pos:pos+32], 'little')); pos += 32
        result["block_hash"] = "0x" + data[pos:pos+32].hex(); pos += 32
        
        available_offset_bytes = len(data) - pos
        if fork in ["deneb", "electra"]:
            available_offset_bytes -= 16
        
        num_offsets_available = available_offset_bytes // 4
        
        offsets["transactions"] = read_uint32_at(data, pos); pos += 4
        
        if fork in ["capella", "deneb", "electra"] and num_offsets_available >= 2:
            offsets["withdrawals"] = read_uint32_at(data, pos); pos += 4
        
        if fork in ["deneb", "electra"]:
            result["blob_gas_used"] = str(read_uint64_at(data, pos)); pos += 8
            result["excess_blob_gas"] = str(read_uint64_at(data, pos)); pos += 8
        
        return result, pos, offsets
    
    def parse_transaction_data(self, data: bytes) -> str:
        """Parse complete transaction data to hex string"""
        return "0x" + data.hex()

    def parse_execution_payload_variable_fields(self, data: bytes, offsets: Dict[str, int], 
                                         variable_fields: List[str]) -> Dict[str, Any]:
        """Parse variable fields in execution payload"""
        result = {}
        
        if ("transactions" in offsets and "withdrawals" in offsets and 
            offsets["transactions"] == offsets["withdrawals"]):
            result["transactions"] = []
            
            for field_name in variable_fields:
                if field_name == "transactions":
                    continue
                elif field_name not in offsets:
                    continue
                    
                start = offsets[field_name]
                end = len(data)
                sorted_offsets = sorted([v for v in offsets.values() if v > start])
                if sorted_offsets: 
                    end = sorted_offsets[0]
                
                field_data = data[start:end]
                
                if field_name == "extra_data": 
                    result["extra_data"] = "0x" + field_data.hex()
                elif field_name == "withdrawals": 
                    result["withdrawals"] = parse_list_of_items(field_data, self.parse_withdrawal)
            
            return result
        
        for field_name in variable_fields:
            if field_name not in offsets:
                continue
                
            start = offsets[field_name]
            end = len(data)
            sorted_offsets = sorted([v for v in offsets.values() if v > start])
            if sorted_offsets: 
                end = sorted_offsets[0]
            
            field_data = data[start:end]
            
            if field_name == "extra_data": 
                result["extra_data"] = "0x" + field_data.hex()
            elif field_name == "transactions": 
                result["transactions"] = parse_list_of_items(field_data, self.parse_transaction_data)
            elif field_name == "withdrawals": 
                result["withdrawals"] = parse_list_of_items(field_data, self.parse_withdrawal)
        
        return result

    def parse_variable_field_data(self, body_data: bytes, all_offsets: List[int], 
                                 field_definitions: List[tuple]) -> Dict[str, Any]:
        """Parse variable field data using offsets"""
        result = {}
        
        for i, ((field_name, parser_func, *args), offset) in enumerate(zip(field_definitions, all_offsets)):
            end = len(body_data)
            next_offsets = [o for o in all_offsets if o > offset]
            if next_offsets:
                end = min(next_offsets)
            
            if i + 1 < len(all_offsets) and offset == all_offsets[i + 1]:
                if field_name in ["sync_aggregate", "execution_payload"]:
                    result[field_name] = {}
                elif field_name == "execution_requests":
                    result[field_name] = {"deposits": [], "withdrawals": [], "consolidations": []}
                else:
                    result[field_name] = []
                continue
            
            if offset >= len(body_data) or end <= offset:
                if field_name in ["sync_aggregate", "execution_payload"]:
                    result[field_name] = {}
                elif field_name == "execution_requests":
                    result[field_name] = {"deposits": [], "withdrawals": [], "consolidations": []}
                else:
                    result[field_name] = []
                continue

            field_data = body_data[offset:end]
            
            try:
                if parser_func == parse_list_of_items:
                    result[field_name] = parser_func(field_data, *args)
                else:
                    result[field_name] = parser_func(field_data, *args)
                    
            except Exception as e:
                print(f"❌ Error parsing field {field_name}: {e}")
                
                if field_name in ["sync_aggregate", "execution_payload"]:
                    result[field_name] = {}
                elif field_name == "execution_requests":
                    result[field_name] = {"deposits": [], "withdrawals": [], "consolidations": []}
                else:
                    result[field_name] = []

        return result
    
    def ensure_all_fields(self, result: Dict[str, Any], expected_fields: List[str]) -> Dict[str, Any]:
        """Ensure all expected fields are present with default values"""
        for field_name in expected_fields:
            if field_name not in result:
                if field_name in ["sync_aggregate", "execution_payload"]:
                    result[field_name] = {}
                elif field_name == "execution_requests":
                    result[field_name] = {"deposits": [], "withdrawals": [], "consolidations": []}
                else:
                    result[field_name] = []
        return result
    
    def get_base_field_definitions(self) -> List[tuple]:
        """Get the 5 base variable field definitions common to all forks"""
        return [
            ("proposer_slashings", parse_list_of_items, self.parse_proposer_slashing),
            ("attester_slashings", parse_list_of_items, self.parse_attester_slashing),
            ("attestations", parse_list_of_items, self.parse_attestation),
            ("deposits", parse_list_of_items, self.parse_deposit),
            ("voluntary_exits", parse_list_of_items, self.parse_voluntary_exit)
        ]
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse beacon block body using schema-based approach"""
        return self._parse_body_from_schema(body_data)