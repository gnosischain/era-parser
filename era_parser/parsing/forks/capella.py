"""Capella fork parser - Declarative approach"""

from typing import Dict, Any
from ..ssz_utils import parse_list_of_items
from .bellatrix import BellatrixParser

class CapellaParser(BellatrixParser):
    """Parser for Capella fork blocks - adds withdrawals and BLS changes"""
    
    # Inherit Bellatrix schema and add BLS changes
    BODY_SCHEMA = BellatrixParser.BODY_SCHEMA + [
        ('variable', 'bls_to_execution_changes', (parse_list_of_items, 'parse_bls_to_execution_change')),
    ]
    
    def parse_execution_payload_bellatrix(self, data: bytes) -> Dict[str, Any]:
        """Parse execution_payload for Capella (overrides Bellatrix)"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data, "capella")
            variable_fields = ["extra_data", "transactions", "withdrawals"]
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            return result
        except Exception:
            return {}