import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple

from ..ssz_utils import read_uint32_at, read_uint64_at, parse_list_of_items

class BaseForkParser(ABC):
    """Base class for all fork parsers with common parsing logic"""
    
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
        # Read offsets for base 5 variable fields
        base_offsets = []
        for i in range(5):
            offset = read_uint32_at(body_data, start_pos + i * 4)
            base_offsets.append(offset)
        
        return base_offsets, start_pos + 20  # 5 offsets * 4 bytes
    
    # Common parsing methods used across forks
    def parse_deposit(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single deposit"""
        try:
            if len(data) < 1240:
                return None
            
            pos = 0
            
            # Parse proof: Vector[Bytes32, 33] - exactly 33 hashes of 32 bytes each
            proof = []
            for i in range(33):
                if pos + 32 > len(data):
                    return None
                hash_bytes = data[pos:pos+32]
                proof.append("0x" + hash_bytes.hex())
                pos += 32
            
            # Parse deposit data (should be exactly 184 bytes remaining)
            if pos + 184 > len(data):
                return None
            
            deposit_data_raw = data[pos:pos+184]
            data_pos = 0
            
            # pubkey: Bytes48 (48 bytes)
            pubkey = "0x" + deposit_data_raw[data_pos:data_pos+48].hex()
            data_pos += 48
            
            # withdrawal_credentials: Bytes32 (32 bytes)
            withdrawal_credentials = "0x" + deposit_data_raw[data_pos:data_pos+32].hex()
            data_pos += 32
            
            # amount: uint64 (8 bytes)
            amount = str(read_uint64_at(deposit_data_raw, data_pos))
            data_pos += 8
            
            # signature: Bytes96 (96 bytes)
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

    def parse_attestation(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single attestation"""
        try:
            if len(data) < 228:  # Minimum size
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

    def parse_indexed_attestation(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse an IndexedAttestation (used in AttesterSlashing)"""
        try:
            if len(data) < 232:  # Minimum size: 4 (offset) + 128 (AttestationData) + 96 (signature) + 4 (min indices)
                return None
            
            # Read offset for attesting_indices
            indices_offset = read_uint32_at(data, 0)
            
            if indices_offset >= len(data):
                return None
            
            # Parse AttestationData (fixed 128 bytes after offset)
            att_data_start = 4  # After the offset
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
            
            # Parse signature (96 bytes after AttestationData)
            signature_start = att_data_start + 128
            signature = "0x" + data[signature_start:signature_start + 96].hex()
            
            # Parse attesting_indices (variable length list at offset)
            indices_data = data[indices_offset:]
            attesting_indices = []
            
            # Parse the list of validator indices as consecutive uint64 values
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

    def parse_proposer_slashing(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single proposer slashing"""
        try:
            if len(data) < 416:  # 2 * (8+8+32+32+32+96) = 2 * 208 = 416 bytes
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
            pos += 96
            
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

    def parse_attester_slashing(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse a single attester slashing - FIXED to use IndexedAttestation"""
        try:
            if len(data) < 8:  # Need at least 2 offsets
                return None
            
            # Attester slashings have two IndexedAttestations, so we need to parse offsets
            attestation_1_offset = read_uint32_at(data, 0)
            attestation_2_offset = read_uint32_at(data, 4)
            
            if attestation_1_offset >= len(data) or attestation_2_offset >= len(data):
                return None
            
            # Parse attestation_1 (IndexedAttestation)
            attestation_1_end = attestation_2_offset
            attestation_1_data = data[attestation_1_offset:attestation_1_end]
            attestation_1 = self.parse_indexed_attestation(attestation_1_data)
            
            # Parse attestation_2 (IndexedAttestation)
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

    def parse_voluntary_exit(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse a single voluntary exit - FIXED version for proper parsing
        Expected structure: 8 bytes epoch + 8 bytes validator_index + 96 bytes signature = 112 bytes total
        """
        try:
            if len(data) < 112:  # Must be exactly 112 bytes
                return None
            
            pos = 0
            
            # Parse the message part (16 bytes total)
            epoch = str(read_uint64_at(data, pos))
            pos += 8
            validator_index = str(read_uint64_at(data, pos))
            pos += 8
            
            # Parse signature (96 bytes)
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

    def parse_bls_to_execution_change(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse a single BLS to execution change (Capella+) - FIXED to handle proper structure
        Expected: validator_index (8) + from_bls_pubkey (48) + to_execution_address (20) + signature (96) = 172 bytes
        """
        try:
            if len(data) < 172:  # Must be exactly 172 bytes
                return None
            
            pos = 0
            
            # Parse the message part (76 bytes total)
            validator_index = str(read_uint64_at(data, pos))
            pos += 8
            
            from_bls_pubkey = "0x" + data[pos:pos + 48].hex()
            pos += 48
            
            to_execution_address = "0x" + data[pos:pos + 20].hex()
            pos += 20
            
            # Parse signature (96 bytes)
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
    
    def parse_sync_aggregate(self, data: bytes) -> Dict[str, Any]:
        """Parse sync_aggregate - fixed 160-byte structure (Altair+)"""
        if len(data) < 160: 
            return {}
        return {
            "sync_committee_bits": "0x" + data[0:64].hex(), 
            "sync_committee_signature": "0x" + data[64:160].hex()
        }
    
    def parse_withdrawal(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse execution payload withdrawal - 44 bytes fixed size (Capella+)"""
        if len(data) < 44:
            return None
        return {
            "index": str(read_uint64_at(data, 0)), 
            "validator_index": str(read_uint64_at(data, 8)),
            "address": "0x" + data[16:36].hex(), 
            "amount": str(read_uint64_at(data, 36))
        }
    
    def parse_kzg_commitment(self, data: bytes) -> Optional[str]:
        """Parse KZG commitment - 48 bytes fixed size (Deneb+)"""
        if len(data) != 48:
            return None
        return "0x" + data.hex()
    
    # Execution payload parsing with version differences
    def parse_execution_payload_base(self, data: bytes) -> Tuple[Dict[str, Any], int, Dict[str, int]]:
        """Parse the common part of execution payload (Bellatrix base)"""
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
        
        # Variable fields offsets
        offsets = {}
        offsets["extra_data"] = read_uint32_at(data, pos); pos += 4
        result["base_fee_per_gas"] = str(int.from_bytes(data[pos:pos+32], 'little')); pos += 32
        result["block_hash"] = "0x" + data[pos:pos+32].hex(); pos += 32
        offsets["transactions"] = read_uint32_at(data, pos); pos += 4
        
        return result, pos, offsets
    
    def parse_transaction_data(self, data: bytes) -> str:
        """Parse complete transaction data to hex string"""
        return "0x" + data.hex()
    
    def parse_execution_payload_variable_fields(self, data: bytes, offsets: Dict[str, int], 
                                              variable_fields: List[str]) -> Dict[str, Any]:
        """Parse variable fields in execution payload"""
        result = {}
        
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
        """Parse variable field data using offsets - FIXED for better debugging"""
        result = {}
        
        for i, ((field_name, parser_func, *args), offset) in enumerate(zip(field_definitions, all_offsets)):
            # Find end position
            end = len(body_data)
            next_offsets = [o for o in all_offsets if o > offset]
            if next_offsets:
                end = min(next_offsets)
            
            # FIXED: Check if this field is empty (same offset as next field)
            if i + 1 < len(all_offsets) and offset == all_offsets[i + 1]:
                # This field is empty - same offset as next field
                if field_name in ["sync_aggregate", "execution_payload"]:
                    result[field_name] = {}
                elif field_name == "execution_requests":
                    result[field_name] = {"deposits": [], "withdrawals": [], "consolidations": []}
                else:
                    result[field_name] = []
                continue
            
            if offset >= len(body_data) or end <= offset:
                # Handle missing/empty fields
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
                print(f"Error parsing field {field_name}: {e}")
                # Set default values on parse error
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
    
    # Get base field definitions (used by all forks)
    def get_base_field_definitions(self) -> List[tuple]:
        """Get the 5 base variable field definitions common to all forks"""
        return [
            ("proposer_slashings", parse_list_of_items, self.parse_proposer_slashing),
            ("attester_slashings", parse_list_of_items, self.parse_attester_slashing),
            ("attestations", parse_list_of_items, self.parse_attestation),
            ("deposits", parse_list_of_items, self.parse_deposit),
            ("voluntary_exits", parse_list_of_items, self.parse_voluntary_exit)
        ]
    
    @abstractmethod
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse beacon block body for this fork"""
        pass