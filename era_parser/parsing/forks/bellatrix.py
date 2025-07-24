"""Bellatrix fork parser"""

from typing import Dict, Any
from ..ssz_utils import parse_list_of_items, read_uint32_at, read_uint64_at
from .altair import AltairParser

class BellatrixParser(AltairParser):
    """Parser for Bellatrix fork blocks"""
    
    def parse_execution_payload(self, data: bytes, fork: str = "bellatrix") -> Dict[str, Any]:
        """Parse execution_payload - variable-size structure"""
        if len(data) < 100:  # Minimum size check
            return {}
            
        try:
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
            
            offsets, variable_fields = {}, ["extra_data", "transactions"]
            offsets["extra_data"] = read_uint32_at(data, pos); pos += 4
            result["base_fee_per_gas"] = str(int.from_bytes(data[pos:pos+32], 'little')); pos += 32
            result["block_hash"] = "0x" + data[pos:pos+32].hex(); pos += 32
            offsets["transactions"] = read_uint32_at(data, pos); pos += 4
            
            for i, field_name in enumerate(variable_fields):
                start = offsets[field_name]
                end = len(data)
                sorted_offsets = sorted([v for v in offsets.values() if v > start])
                if sorted_offsets: 
                    end = sorted_offsets[0]
                
                field_data = data[start:end]
                
                if field_name == "extra_data": 
                    result["extra_data"] = "0x" + field_data.hex()
                elif field_name == "transactions": 
                    result["transactions"] = parse_list_of_items(field_data, lambda d: "0x" + d.hex())
                    
            return result
            
        except Exception:
            return {}
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse Bellatrix beacon block body"""
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
        
        # Handle sync_aggregate (FIXED SIZE, embedded inline)
        if pos + 160 <= len(body_data):
            sync_aggregate_data = body_data[pos:pos+160]
            result["sync_aggregate"] = self.parse_sync_aggregate(sync_aggregate_data)
            pos += 160
        else:
            result["sync_aggregate"] = {}
        
        # Handle execution_payload (VARIABLE SIZE, uses offset)
        post_merge_offsets = []
        post_merge_fields = []
        
        execution_payload_offset = read_uint32_at(body_data, pos)
        post_merge_offsets.append(execution_payload_offset)
        post_merge_fields.append(("execution_payload", self.parse_execution_payload, "bellatrix"))
        pos += 4
        
        # Combine all offsets and fields
        all_offsets = base_offsets + post_merge_offsets
        base_field_definitions = [
            ("proposer_slashings", parse_list_of_items, lambda d: None),
            ("attester_slashings", parse_list_of_items, lambda d: None),
            ("attestations", parse_list_of_items, self.parse_attestation),
            ("deposits", parse_list_of_items, self.parse_deposit),  # ✅ FIXED: Now using parse_deposit
            ("voluntary_exits", parse_list_of_items, lambda d: None)
        ]
        all_field_definitions = base_field_definitions + post_merge_fields
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, all_offsets, all_field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate", "execution_payload"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result