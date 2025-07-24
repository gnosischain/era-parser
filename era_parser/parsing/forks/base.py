"""Base fork parser with common functionality"""

import json
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

from ..ssz_utils import read_uint32_at, read_uint64_at, parse_list_of_items

class BaseForkParser(ABC):
    """Base class for all fork parsers"""
    
    @abstractmethod
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse beacon block body for this fork"""
        pass
    
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
 
    def parse_deposit(self, data: bytes) -> Optional[Dict[str, Any]]:
        """
        Parse a single deposit
        
        Structure:
        - proof: 33 × 32 bytes = 1056 bytes (Vector[Bytes32, 33])
        - deposit_data: 184 bytes
        - pubkey: 48 bytes
        - withdrawal_credentials: 32 bytes  
        - amount: 8 bytes
        - signature: 96 bytes
        """
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
    
    def parse_variable_field_data(self, body_data: bytes, all_offsets: List[int], 
                                 field_definitions: List[tuple]) -> Dict[str, Any]:
        """Parse variable field data using offsets"""
        result = {}
        
        for i, ((field_name, parser_func, *args), offset) in enumerate(zip(field_definitions, all_offsets)):
            # Find end position
            end = len(body_data)
            next_offsets = [o for o in all_offsets if o > offset]
            if next_offsets:
                end = min(next_offsets)
            
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
            except Exception:
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