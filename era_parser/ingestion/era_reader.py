import struct
import os
from typing import List, Tuple, Optional, NamedTuple
from dataclasses import dataclass

from .compression import decompress_snappy_framed
from ..parsing.ssz_utils import read_uint64_at, read_uint32_at
from ..config import get_network_config

@dataclass
class EraRecord:
    """Represents a record in an era file"""
    slot: int
    data: bytes
    record_type: str

class EraReader:
    """Reader for era files"""
    
    def __init__(self, filepath: str, network: str = None):
        """
        Initialize era reader
        
        Args:
            filepath: Path to era file
            network: Network name (auto-detected if None)
        """
        self.filepath = filepath
        self.network = network or self._detect_network()
        self.config = get_network_config(self.network)
        
    def _detect_network(self) -> str:
        """Detect network from filename"""
        filename = os.path.basename(self.filepath).lower()
        if 'gnosis' in filename:
            return 'gnosis'
        elif 'sepolia' in filename:
            return 'sepolia'
        else:
            return 'mainnet'
    
    def get_era_info(self) -> dict:
        """Extract era information from filename"""
        filename = os.path.basename(self.filepath)
        parts = filename.replace('.era', '').split('-')
        
        era_number = None
        era_hash = 'unknown'
        
        if len(parts) >= 2:
            try:
                # Handle different formats:
                # gnosis-00001.era -> parts = ['gnosis', '00001']
                # gnosis-00001-hash.era -> parts = ['gnosis', '00001', 'hash']
                era_str = parts[1]  # Always take the second part as era number
                era_number = int(era_str)
                
                if len(parts) > 2:
                    era_hash = parts[2]
                    
            except (ValueError, IndexError):
                print(f"⚠️  Could not parse era number from filename: {filename}")
                era_number = 0
        
        if era_number is not None:
            start_slot = era_number * self.config['SLOTS_PER_HISTORICAL_ROOT']
            end_slot = start_slot + self.config['SLOTS_PER_HISTORICAL_ROOT'] - 1
        else:
            start_slot = None
            end_slot = None
            era_number = 0
        
        result = {
            'era_number': era_number,
            'start_slot': start_slot,
            'end_slot': end_slot,
            'network': self.network,
            'hash': era_hash,
            'filename': filename
        }
        
        print(f"🔍 Era info extracted: era {era_number}, slots {start_slot}-{end_slot}, network {self.network}")
        
        return result
    
    def read_all_records(self) -> List[EraRecord]:
        """Read all records from era file"""
        records = []
        
        with open(self.filepath, "rb") as f:
            # Skip version header
            f.seek(8)
            
            while True:
                # Read record header
                header = f.read(8)
                if len(header) < 8:
                    break
                    
                record_type = header[0:2]
                data_length = struct.unpack("<I", header[2:6])[0]
                
                if data_length == 0:
                    continue
                    
                # Read record data
                record_data = f.read(data_length)
                if len(record_data) < data_length:
                    break
                
                # Determine record type and extract slot if it's a block
                if record_type == b'\x01\x00':  # CompressedSignedBeaconBlock
                    try:
                        # Extract slot from the compressed block
                        decompressed = decompress_snappy_framed(record_data)
                        message_offset = read_uint32_at(decompressed, 0)
                        message_data = decompressed[message_offset:]
                        slot = read_uint64_at(message_data, 0)
                        records.append(EraRecord(slot, record_data, "block"))
                    except Exception:
                        continue
                elif record_type == b'\x02\x00':  # CompressedBeaconState
                    records.append(EraRecord(0, record_data, "state"))
                elif record_type == b'\x69\x32':  # SlotIndex
                    records.append(EraRecord(0, record_data, "index"))
        
        return records
    
    def get_block_records(self) -> List[Tuple[int, bytes]]:
        """Get only block records sorted by slot"""
        records = self.read_all_records()
        block_records = [(record.slot, record.data) for record in records if record.record_type == "block"]
        return sorted(block_records, key=lambda x: x[0])
    
    def get_statistics(self) -> dict:
        """Get statistics about the era file"""
        records = self.read_all_records()
        
        block_count = sum(1 for r in records if r.record_type == "block")
        state_count = sum(1 for r in records if r.record_type == "state")
        index_count = sum(1 for r in records if r.record_type == "index")
        
        stats = {
            'total_records': len(records),
            'blocks': block_count,
            'states': state_count,
            'indices': index_count
        }
        
        if block_count > 0:
            block_slots = [r.slot for r in records if r.record_type == "block"]
            stats['min_slot'] = min(block_slots)
            stats['max_slot'] = max(block_slots)
        
        return stats