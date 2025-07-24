"""Bellatrix fork parser - Only adds execution_payload to Altair"""

from typing import Dict, Any
from ..ssz_utils import read_uint32_at
from .altair import AltairParser

class BellatrixParser(AltairParser):
    """Parser for Bellatrix fork blocks - adds execution_payload"""
    
    def parse_execution_payload(self, data: bytes, fork: str = "bellatrix") -> Dict[str, Any]:
        """Parse execution_payload for Bellatrix (no withdrawals or blob gas)"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data)
            
            # Bellatrix only has: extra_data, transactions
            variable_fields = ["extra_data", "transactions"]
            
            # Parse variable fields
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            
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
        
        # Inherited from Altair: sync_aggregate (FIXED SIZE, embedded inline)
        if pos + 160 <= len(body_data):
            sync_aggregate_data = body_data[pos:pos+160]
            result["sync_aggregate"] = self.parse_sync_aggregate(sync_aggregate_data)
            pos += 160
        else:
            result["sync_aggregate"] = {}
        
        # NEW in Bellatrix: execution_payload (VARIABLE SIZE, uses offset)
        execution_payload_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # Combine all offsets and fields
        all_offsets = base_offsets + [execution_payload_offset]
        all_field_definitions = self.get_base_field_definitions() + [
            ("execution_payload", self.parse_execution_payload, "bellatrix")
        ]
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, all_offsets, all_field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present (Altair + execution_payload)
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate", "execution_payload"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result