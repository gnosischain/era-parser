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
        """Parse ExecutionRequests"""
        if len(data) < 12:
            return {"deposits": [], "withdrawals": [], "consolidations": []}
        
        try:
            deposits_offset = read_uint32_at(data, 0)
            withdrawals_offset = read_uint32_at(data, 4) 
            consolidations_offset = read_uint32_at(data, 8)
            
            result = {"deposits": [], "withdrawals": [], "consolidations": []}
            
            if deposits_offset < len(data):
                end_offset = min([o for o in [withdrawals_offset, consolidations_offset, len(data)] if o > deposits_offset])
                deposits_data = data[deposits_offset:end_offset]
                result["deposits"] = parse_list_of_items(deposits_data, self.parse_deposit_request)
            
            if withdrawals_offset < len(data):
                end_offset = min([o for o in [consolidations_offset, len(data)] if o > withdrawals_offset])
                withdrawals_data = data[withdrawals_offset:end_offset]
                result["withdrawals"] = parse_list_of_items(withdrawals_data, self.parse_withdrawal_request)
                
            if consolidations_offset < len(data):
                consolidations_data = data[consolidations_offset:]
                result["consolidations"] = parse_list_of_items(consolidations_data, self.parse_consolidation_request)
                
            return result
            
        except Exception:
            return {"deposits": [], "withdrawals": [], "consolidations": []}