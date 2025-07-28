"""Deneb fork parser - Only adds blob gas and KZG commitments to Capella"""

from typing import Dict, Any
from ..ssz_utils import parse_list_of_items, read_uint32_at, read_uint64_at
from .capella import CapellaParser

class DenebParser(CapellaParser):
    """Parser for Deneb fork blocks - adds blob gas and KZG commitments"""
    
    def parse_execution_payload(self, data: bytes, fork: str = "deneb") -> Dict[str, Any]:
        """Parse execution_payload for Deneb (adds blob gas fields)"""
        try:
            result, pos, offsets = self.parse_execution_payload_base(data, "deneb")
            
            # Deneb has: extra_data, transactions, withdrawals (same as Capella)
            variable_fields = ["extra_data", "transactions", "withdrawals"]
            
            # Parse variable fields
            variable_result = self.parse_execution_payload_variable_fields(data, offsets, variable_fields)
            result.update(variable_result)
            
            return result
            
        except Exception:
            return {}
    
    def parse_body(self, body_data: bytes) -> Dict[str, Any]:
        """Parse Deneb beacon block body"""
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
        
        # NEW in Deneb: blob_kzg_commitments - FIXED: Now properly parses KZG commitments
        blob_commitments_offset = read_uint32_at(body_data, pos)
        pos += 4
        
        # Combine all offsets and fields
        all_offsets = base_offsets + [execution_payload_offset, bls_changes_offset, blob_commitments_offset]
        all_field_definitions = self.get_base_field_definitions() + [
            ("execution_payload", self.parse_execution_payload, "deneb"),
            ("bls_to_execution_changes", parse_list_of_items, self.parse_bls_to_execution_change),
            ("blob_kzg_commitments", parse_list_of_items, self.parse_kzg_commitment)
        ]
        
        # Parse variable fields
        parsed_fields = self.parse_variable_field_data(body_data, all_offsets, all_field_definitions)
        result.update(parsed_fields)
        
        # Ensure all expected fields are present (Capella + blob_kzg_commitments)
        expected_fields = [
            "proposer_slashings", "attester_slashings", "attestations", 
            "deposits", "voluntary_exits", "sync_aggregate", 
            "execution_payload", "bls_to_execution_changes", "blob_kzg_commitments"
        ]
        result = self.ensure_all_fields(result, expected_fields)
        
        return result