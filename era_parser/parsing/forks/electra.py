"""Electra fork parser - Only adds execution requests to Deneb"""

from typing import Dict, Any, Optional
from ..ssz_utils import parse_list_of_items, read_uint32_at, read_uint64_at
from .deneb import DenebParser

class ElectraParser(DenebParser):
    """Parser for Electra fork blocks - adds execution requests"""
    
    def parse_deposit_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse DepositRequest - 192 bytes fixed size"""
        if len(data) < 192:
            return None
        return {
            "pubkey": "0x" + data[0:48].hex(),
            "withdrawal_credentials": "0x" + data[48:80].hex(),
            "amount": str(read_uint64_at(data, 80)),
            "signature": "0x" + data[88:184].hex(),
            "index": str(read_uint64_at(data, 184))
        }

    def parse_withdrawal_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse WithdrawalRequest - 76 bytes fixed size"""
        if len(data) < 76:
            return None
        return {
            "source_address": "0x" + data[0:20].hex(),
            "validator_pubkey": "0x" + data[20:68].hex(),
            "amount": str(read_uint64_at(data, 68))
        }

    def parse_consolidation_request(self, data: bytes) -> Optional[Dict[str, Any]]:
        """Parse ConsolidationRequest - 116 bytes fixed size"""
        if len(data) < 116:
            return None
        return {
            "source_address": "0x" + data[0:20].hex(),
            "source_pubkey": "0x" + data[20:68].hex(),
            "target_pubkey": "0x" + data[68:116].hex()
        }

    def parse_execution_requests(self, data: bytes) -> Dict[str, Any]:
        """Parse ExecutionRequests - variable-size container with 3 lists"""
        if len(data) < 12:  # Minimum size: 3 offsets * 4 bytes each
            return {"deposits": [], "withdrawals": [], "consolidations": []}
        
        try:
            # Read the three offsets (12 bytes total)
            deposits_offset = read_uint32_at(data, 0)
            withdrawals_offset = read_uint32_at(data, 4) 
            consolidations_offset = read_uint32_at(data, 8)
            
            result = {"deposits": [], "withdrawals": [], "consolidations": []}
            
            # Parse deposits
            if deposits_offset < len(data):
                end_offset = min([o for o in [withdrawals_offset, consolidations_offset, len(data)] if o > deposits_offset])
                deposits_data = data[deposits_offset:end_offset]
                result["deposits"] = parse_list_of_items(deposits_data, self.parse_deposit_request)
            
            # Parse withdrawals  
            if withdrawals_offset < len(data):
                end_offset = min([o for o in [consolidations_offset, len(data)] if o > withdrawals_offset])
                withdrawals_data = data[withdrawals_offset:end_offset]
                result["withdrawals"] = parse_list_of_items(withdrawals_data, self.parse_withdrawal_request)
                
            # Parse consolidations
            if consolidations_offset < len(data):
                consolidations_data = data[consolidations_offset:]
                result["consolidations"] = parse_list_of_items(consolidations_data, self.parse_consolidation_request)
                
            return result
            
        except Exception:
            return {"deposits": [], "withdrawals": [], "consolidations": []}
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse Electra beacon block body"""
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
        
        # Inherited from Bellatrix: execution_payload
        execution_payload_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # Inherited from Capella: bls_to_execution_changes
        bls_changes_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # Inherited from Deneb: blob_kzg_commitments
        blob_commitments_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # NEW in Electra: execution_requests
        execution_requests_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # Combine all offsets and fields
        all_offsets = base_offsets + [execution_payload_offset, bls_changes_offset, 
                                     blob_commitments_offset, execution_requests_offset]
        all_field_definitions = self.get_base_field_definitions() + [
            ("execution_payload", self.parse_execution_payload, "deneb"),
            ("bls_to_execution_changes", parse_list_of_items, lambda d: None),
            ("blob_kzg_commitments", parse_list_of_items, self.parse_kzg_commitment),
            ("execution_requests", self.parse_execution_requests)
        ]
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, all_offsets, all_field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate", 
            "execution_payload", "bls_to_execution_changes", "blob_kzg_commitments",
            "execution_requests"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result