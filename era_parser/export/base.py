"""Base exporter interface"""

import json
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timezone

class BaseExporter(ABC):
    """Base class for all exporters"""
    
    def __init__(self, era_info: Dict[str, Any]):
        """
        Initialize exporter
        
        Args:
            era_info: Era information dictionary
        """
        self.era_info = era_info
    
    @abstractmethod
    def export_blocks(self, blocks: List[Dict[str, Any]], output_file: str):
        """Export complete blocks"""
        pass
    
    @abstractmethod
    def export_data_type(self, data: List[Dict[str, Any]], output_file: str, data_type: str):
        """Export specific data type"""
        pass
    
    def create_metadata(self, data_count: int, data_type: str = "blocks") -> Dict[str, Any]:
        """Create common metadata"""
        return {
            "era_info": self.era_info,
            "data_type": data_type,
            "record_count": data_count,
            "export_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def flatten_block_for_table(self, block: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten block structure for tabular formats"""
        message = block.get("data", {}).get("message", {})
        body = message.get("body", {})
        execution_payload = body.get("execution_payload", {})
        
        flattened = {
            # Block basics
            "slot": message.get("slot"),
            "proposer_index": message.get("proposer_index"),
            "parent_root": message.get("parent_root"),
            "state_root": message.get("state_root"),
            "signature": block.get("data", {}).get("signature"),
            
            # Block metadata
            "version": block.get("version"),
            "timestamp_utc": block.get("timestamp_utc"),
            "compressed_size": block.get("metadata", {}).get("compressed_size"),
            "decompressed_size": block.get("metadata", {}).get("decompressed_size"),
            
            # Body basics
            "randao_reveal": body.get("randao_reveal"),
            "graffiti": body.get("graffiti"),
            
            # ETH1 data
            "eth1_deposit_root": body.get("eth1_data", {}).get("deposit_root"),
            "eth1_deposit_count": body.get("eth1_data", {}).get("deposit_count"),
            "eth1_block_hash": body.get("eth1_data", {}).get("block_hash"),
            
            # Counts
            "attestation_count": len(body.get("attestations", [])),
            "proposer_slashing_count": len(body.get("proposer_slashings", [])),
            "attester_slashing_count": len(body.get("attester_slashings", [])),
            "deposit_count": len(body.get("deposits", [])),
            "voluntary_exit_count": len(body.get("voluntary_exits", [])),
            "bls_change_count": len(body.get("bls_to_execution_changes", [])),
            "blob_commitment_count": len(body.get("blob_kzg_commitments", [])),
            
            # Sync aggregate
            "sync_committee_bits": body.get("sync_aggregate", {}).get("sync_committee_bits"),
            "sync_committee_signature": body.get("sync_aggregate", {}).get("sync_committee_signature"),
            
            # Execution payload
            "parent_hash": execution_payload.get("parent_hash"),
            "fee_recipient": execution_payload.get("fee_recipient"),
            "execution_state_root": execution_payload.get("state_root"),
            "receipts_root": execution_payload.get("receipts_root"),
            "logs_bloom": execution_payload.get("logs_bloom"),
            "prev_randao": execution_payload.get("prev_randao"),
            "block_number": execution_payload.get("block_number"),
            "gas_limit": execution_payload.get("gas_limit"),
            "gas_used": execution_payload.get("gas_used"),
            "timestamp": execution_payload.get("timestamp"),
            "base_fee_per_gas": execution_payload.get("base_fee_per_gas"),
            "block_hash": execution_payload.get("block_hash"),
            "blob_gas_used": execution_payload.get("blob_gas_used"),
            "excess_blob_gas": execution_payload.get("excess_blob_gas"),
            "extra_data": execution_payload.get("extra_data"),
            
            # Actual data as JSON strings for tabular formats
            "transactions": json.dumps(execution_payload.get("transactions", [])),
            "withdrawals": json.dumps(execution_payload.get("withdrawals", [])),
            "attestations": json.dumps(body.get("attestations", [])),
            "execution_requests": json.dumps(body.get("execution_requests", {})),
            "bls_to_execution_changes": json.dumps(body.get("bls_to_execution_changes", [])),
            "blob_kzg_commitments": json.dumps(body.get("blob_kzg_commitments", [])),
            
            # Counts for reference
            "transaction_count": len(execution_payload.get("transactions", [])),
            "withdrawal_count": len(execution_payload.get("withdrawals", [])),
            "deposit_request_count": len(body.get("execution_requests", {}).get("deposits", [])),
            "withdrawal_request_count": len(body.get("execution_requests", {}).get("withdrawals", [])),
            "consolidation_request_count": len(body.get("execution_requests", {}).get("consolidations", [])),
        }
        
        return flattened