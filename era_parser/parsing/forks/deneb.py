from typing import Dict, Any
from ..ssz_utils import parse_list_of_items
from .capella import CapellaParser

class DenebParser(CapellaParser):
    """Parser for Deneb fork blocks - adds blob KZG commitments"""
    
    # Inherit Capella schema and add blob commitments
    BODY_SCHEMA = CapellaParser.BODY_SCHEMA + [
        ('variable', 'blob_kzg_commitments', (parse_list_of_items, 'parse_kzg_commitment')),
    ]
    
    def parse_execution_payload_bellatrix(self, data: bytes) -> Dict[str, Any]:
        """Parse execution_payload for Deneb (overrides Capella)"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data, "deneb")
            variable_fields = ["extra_data", "transactions", "withdrawals"]
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            return result
        except Exception:
            return {}