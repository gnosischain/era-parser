"""Altair fork parser"""

from typing import Dict, Any
from ..ssz_utils import parse_list_of_items
from .base import BaseForkParser

class AltairParser(BaseForkParser):
    """Parser for Altair fork blocks"""
    
    def parse_sync_aggregate(self, data: bytes) -> Dict[str, Any]:
        """Parse sync_aggregate - fixed 160-byte structure"""
        if len(data) < 160: 
            return {}
        return {
            "sync_committee_bits": "0x" + data[0:64].hex(), 
            "sync_committee_signature": "0x" + data[64:160].hex()
        }
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse Altair beacon block body"""
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
        
        # Altair has the base 5 variable fields
        field_definitions = [
            ("proposer_slashings", parse_list_of_items, lambda d: None),
            ("attester_slashings", parse_list_of_items, lambda d: None),
            ("attestations", parse_list_of_items, self.parse_attestation),
            ("deposits", parse_list_of_items, lambda d: None),
            ("voluntary_exits", parse_list_of_items, lambda d: None)
        ]
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, base_offsets, field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result