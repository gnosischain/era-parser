"""Phase 0 fork parser"""

from typing import Dict, Any
from ..ssz_utils import parse_list_of_items
from .base import BaseForkParser

class Phase0Parser(BaseForkParser):
    """Parser for Phase 0 blocks"""
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse Phase 0 beacon block body"""
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
        
        # Phase 0 has the base 5 variable fields with actual deposit parsing
        field_definitions = [
            ("proposer_slashings", parse_list_of_items, lambda d: None),
            ("attester_slashings", parse_list_of_items, lambda d: None),
            ("attestations", parse_list_of_items, self.parse_attestation),
            ("deposits", parse_list_of_items, self.parse_deposit),  # ✅ FIXED: Now using parse_deposit
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