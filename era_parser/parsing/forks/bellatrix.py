from typing import Dict, Any
from .altair import AltairParser

class BellatrixParser(AltairParser):
    """Parser for Bellatrix fork blocks - adds execution_payload"""
    
    # Inherit Altair schema and add execution_payload
    BODY_SCHEMA = AltairParser.BODY_SCHEMA + [
        ('variable', 'execution_payload', 'parse_execution_payload_bellatrix'),
    ]
    
    def parse_execution_payload_bellatrix(self, data: bytes) -> Dict[str, Any]:
        """Parse execution_payload for Bellatrix"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data, "bellatrix")
            variable_fields = ["extra_data", "transactions"]
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            return result
        except Exception:
            return {}