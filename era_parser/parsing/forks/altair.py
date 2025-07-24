"""Altair fork parser - Only adds sync_aggregate to Phase 0"""

from typing import Dict, Any
from .phase0 import Phase0Parser

class AltairParser(Phase0Parser):
    """Parser for Altair fork blocks - adds sync_aggregate"""
    
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
        
        # NEW in Altair: sync_aggregate (FIXED SIZE, embedded inline)
        if pos + 160 <= len(body_data):
            sync_aggregate_data = body_data[pos:pos+160]
            result["sync_aggregate"] = self.parse_sync_aggregate(sync_aggregate_data)
            pos += 160
        else:
            result["sync_aggregate"] = {}
        
        # Same 5 base variable fields as Phase 0
        field_definitions = self.get_base_field_definitions()
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, base_offsets, field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present (Phase 0 + sync_aggregate)
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result