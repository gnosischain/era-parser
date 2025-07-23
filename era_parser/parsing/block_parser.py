"""Main block parsing logic with improved timestamp calculation"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone

from ..ingestion.compression import decompress_snappy_framed
from ..config import get_fork_by_slot, get_network_config
from .ssz_utils import read_uint32_at, read_uint64_at
from .forks import get_fork_parser

class BlockParser:
    """Main block parser that delegates to fork-specific parsers"""
    
    def __init__(self, network: str = 'mainnet'):
        """
        Initialize block parser
        
        Args:
            network: Network name for fork detection
        """
        self.network = network
        self.network_config = get_network_config(network)
    
    def parse_block(self, compressed_data: bytes, slot: int) -> Optional[Dict[str, Any]]:
        """
        Parse a complete block from compressed data
        
        Args:
            compressed_data: Compressed block data
            slot: Block slot number
            
        Returns:
            Parsed block dictionary or None if parsing fails
        """
        try:
            # Decompress data
            decompressed_data = decompress_snappy_framed(compressed_data)
            
            # Determine fork
            fork = get_fork_by_slot(slot, self.network)
            
            # Parse basic structure
            message_offset = read_uint32_at(decompressed_data, 0)
            signature = "0x" + decompressed_data[4:100].hex()
            message_data = decompressed_data[message_offset:]
            
            # Parse message header
            slot_parsed = read_uint64_at(message_data, 0)
            proposer_index = read_uint64_at(message_data, 8)
            parent_root = "0x" + message_data[16:48].hex()
            state_root = "0x" + message_data[48:80].hex()
            body_offset = read_uint32_at(message_data, 80)
            
            # Parse body using fork-specific parser
            fork_parser = get_fork_parser(fork)
            body = fork_parser.parse_body(message_data[body_offset:])
            
            # Calculate timestamp with proper fallback
            timestamp_utc = self._calculate_timestamp(slot_parsed, body)
            
            return {
                "data": {
                    "message": {
                        "slot": str(slot_parsed), 
                        "proposer_index": str(proposer_index), 
                        "parent_root": parent_root, 
                        "state_root": state_root, 
                        "body": body
                    }, 
                    "signature": signature
                },
                "execution_optimistic": False, 
                "finalized": True, 
                "version": fork,
                "timestamp_utc": timestamp_utc,
                "metadata": {
                    "compressed_size": len(compressed_data), 
                    "decompressed_size": len(decompressed_data)
                }
            }
            
        except Exception as e:
            print(f"Error parsing block at slot {slot}: {e}")
            return None
    
    def _calculate_timestamp(self, slot: int, body: Dict[str, Any]) -> str:
        """
        Calculate block timestamp with proper fallback logic
        
        Args:
            slot: Block slot number
            body: Parsed block body
            
        Returns:
            ISO formatted timestamp string
        """
        # Method 1: Try to get timestamp from execution payload (Bellatrix+)
        execution_payload = body.get("execution_payload", {})
        if execution_payload:
            timestamp_str = execution_payload.get("timestamp", "0")
            try:
                timestamp = int(timestamp_str)
                if timestamp > 0:
                    return self._format_timestamp(timestamp)
            except (ValueError, TypeError):
                pass
        
        # Method 2: Calculate from genesis time and slot (fallback for all blocks)
        genesis_time = self.network_config['GENESIS_TIME']
        seconds_per_slot = self.network_config['SECONDS_PER_SLOT']
        
        # Calculate block timestamp: genesis_time + (slot * seconds_per_slot)
        block_timestamp = genesis_time + (slot * seconds_per_slot)
        
        return self._format_timestamp(block_timestamp)
    
    def _format_timestamp(self, timestamp: int) -> str:
        """Format timestamp to ISO string"""
        if timestamp > 0:
            return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()
        else:
            return "1970-01-01T00:00:00+00:00"