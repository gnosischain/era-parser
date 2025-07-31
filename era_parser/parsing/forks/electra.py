from typing import Dict, Any, Optional
from ..ssz_utils import parse_list_of_items, read_uint32_at, read_uint64_at
from .deneb import DenebParser

class ElectraParser(DenebParser):
    """Parser for Electra fork blocks - adds execution requests"""
    
    # Inherit Deneb schema and add execution requests
    BODY_SCHEMA = DenebParser.BODY_SCHEMA + [
        ('variable', 'execution_requests', 'parse_execution_requests'),
    ]
    
    def parse_execution_payload_bellatrix(self, data: bytes) -> Dict[str, Any]:
        """Parse execution_payload for Electra (same as Deneb)"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data, "electra")
            variable_fields = ["extra_data", "transactions", "withdrawals"]
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            return result
        except Exception:
            return {}
    
    def parse_deposit_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse DepositRequest"""
        if len(data) < 192:
            return None
        return {
            "pubkey": "0x" + data[0:48].hex(),
            "withdrawal_credentials": "0x" + data[48:80].hex(),
            "amount": str(read_uint64_at(data, 80)),
            "signature": "0x" + data[88:184].hex(),
            "index": str(read_uint64_at(data, 184))
        }
    
    parse_deposit_request.ssz_size = 192

    def parse_withdrawal_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse WithdrawalRequest"""
        if len(data) < 76:
            return None
        return {
            "source_address": "0x" + data[0:20].hex(),
            "validator_pubkey": "0x" + data[20:68].hex(),
            "amount": str(read_uint64_at(data, 68))
        }
    
    parse_withdrawal_request.ssz_size = 76

    def parse_consolidation_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse ConsolidationRequest"""
        if len(data) < 116:
            return None
        return {
            "source_address": "0x" + data[0:20].hex(),
            "source_pubkey": "0x" + data[20:68].hex(),
            "target_pubkey": "0x" + data[68:116].hex()
        }
    
    parse_consolidation_request.ssz_size = 116

    def parse_execution_requests(self, data: bytes) -> Dict[str, Any]:
        """Parse ExecutionRequests with proper SSZ offset handling"""
        if len(data) < 12:
            return {"deposits": [], "withdrawals": [], "consolidations": []}
        
        try:
            deposits_offset = read_uint32_at(data, 0)
            withdrawals_offset = read_uint32_at(data, 4) 
            consolidations_offset = read_uint32_at(data, 8)
            
            result = {"deposits": [], "withdrawals": [], "consolidations": []}
            
            # In SSZ, empty lists all point to the same offset
            # We need to identify unique data sections and parse only the non-empty ones
            unique_offsets = sorted(set([deposits_offset, withdrawals_offset, consolidations_offset]))
            
            # Parse each unique section
            for i, offset in enumerate(unique_offsets):
                if offset >= len(data) or offset < 12:  # Skip invalid or empty sections
                    continue
                    
                # Calculate section end
                section_end = len(data)
                if i + 1 < len(unique_offsets):
                    section_end = unique_offsets[i + 1]
                
                section_data = data[offset:section_end]
                
                if len(section_data) == 0:
                    continue
                    
                # Determine which request type(s) use this offset
                using_this_offset = []
                if deposits_offset == offset:
                    using_this_offset.append("deposits")
                if withdrawals_offset == offset:
                    using_this_offset.append("withdrawals")
                if consolidations_offset == offset:
                    using_this_offset.append("consolidations")
                    
                # If multiple types share the same offset, only the "rightmost" one has data
                if len(using_this_offset) > 1:
                    if "consolidations" in using_this_offset:
                        result["consolidations"] = parse_list_of_items(section_data, self.parse_consolidation_request)
                    elif "withdrawals" in using_this_offset:
                        result["withdrawals"] = parse_list_of_items(section_data, self.parse_withdrawal_request)
                    elif "deposits" in using_this_offset:
                        result["deposits"] = parse_list_of_items(section_data, self.parse_deposit_request)
                else:
                    # Single type uses this offset
                    request_type = using_this_offset[0]
                    
                    if request_type == "deposits":
                        result["deposits"] = parse_list_of_items(section_data, self.parse_deposit_request)
                    elif request_type == "withdrawals":
                        result["withdrawals"] = parse_list_of_items(section_data, self.parse_withdrawal_request)
                    elif request_type == "consolidations":
                        result["consolidations"] = parse_list_of_items(section_data, self.parse_consolidation_request)
            
            return result
            
        except Exception as e:
            return {"deposits": [], "withdrawals": [], "consolidations": []}